//! Local plugin let you load file from your filesystem in rustruct tree
//! It's simply a VFileBuilder wrapper around std:fs::File

use std::fs::{File,Metadata, read_dir};
use std::sync::{Arc};
use seek_bufread::BufReader;

use tap::config_schema;
use tap::plugin;
use tap::plugin::{PluginInfo, PluginInstance, PluginConfig, PluginArgument, PluginResult, PluginEnvironment};
use tap::vfile::{VFile, VFileBuilder};
use tap::tree::{Tree, TreeNodeIdSchema};
use tap::error::RustructError;
use tap::node::Node;
use tap::attribute::Attributes;
use tap::tree::TreeNodeId;
use tap::value::Value;

use serde::{Serialize, Deserialize};
use schemars::{JsonSchema};
use fdlimit::raise_fd_limit;

plugin!("local", "Input", "Load files or directory from the filesystem", Local, Arguments);

#[derive(Default)]
pub struct Local 
{
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Arguments
{
  files : Vec<String>,
  #[schemars(with = "TreeNodeIdSchema")] 
  mount_point : TreeNodeId, 
}

#[derive(Debug, Serialize, Deserialize,Default)]
pub struct Results
{
}

impl Local 
{
  fn create_attributes(&self, metadata : &Metadata) -> Attributes 
  {
    let mut attributes = Attributes::new();
      
    if let Ok(time) = metadata.accessed() 
    {
      attributes.add_attribute("accessed", Value::DateTime(time.into()), None);
    }
    if let Ok(time) = metadata.created() 
    {
      attributes.add_attribute("created", Value::DateTime(time.into()), None);
    }
    if let Ok(time) = metadata.modified() 
    {
      attributes.add_attribute("modified", Value::DateTime(time.into()), None);
    }

    attributes 
  }

  //We don't create file if we can't read it's content !
  fn create_node(&self, file_path : String, metadata : &Metadata) -> Option<Node>
  {
     let file_name = match file_path.rfind('/')
     {
       Some(pos) => &file_path[pos+1..],//fix end ?
       None => &file_path,
     };

     let node = Node::new(String::from(file_name));
     let attributes = self.create_attributes(metadata);
     node.value().add_attribute("local", attributes, None);

     if metadata.is_file()
     {
       let vfile_builder = match LocalVFileBuilder::new(file_path.clone())
       {
         Ok(vfile_builder) => vfile_builder,
         Err(_) => return None,
       };
       node.value().add_attribute("file", None, None);
       node.value().add_attribute("data", Value::VFileBuilder(Arc::new(vfile_builder)), None);
     }
     else {
       node.value().add_attribute("directory", None, None);
     }

     Some(node)
  }

  fn creates(&self, parent_id : TreeNodeId, pathes : Vec<String>, tree : &Tree) -> anyhow::Result<()>
  {
    for path in pathes
    {
      let metadata = match std::fs::metadata(path.clone())
      {
        Ok(metadata) => metadata,
        Err(_) => continue,
      };

      if let Some(node) = self.create_node(path.clone(), &metadata)
      {
         let node_id = tree.add_child(parent_id, node)?;

         if metadata.is_dir()
         {
           if let Ok(entries) = read_dir(path.clone())
           {
             let mut pathes = Vec::new();
             for entry in entries.flatten() 
             {
               if let Some(path) = entry.path().to_str()
               {
                 pathes.push(String::from(path)); 
               }
             }
             self.creates(node_id, pathes, tree)?;
          }
        }
      }
    }
    Ok(())
  }

  fn run(&mut self, args : Arguments, env : PluginEnvironment) -> anyhow::Result<Results>
  {
    let max = raise_fd_limit(); //we raise fd limit even if not much than core num * 2 files should be opened
    log::info!("Max FD {:?}", max);

    self.creates(args.mount_point, args.files, &env.tree)?;

    Ok(Results{})
  }
}

#[derive(Debug,Serialize,Deserialize)]
pub struct LocalVFileBuilder
{
  file_path : String,
  size : u64,
}

impl LocalVFileBuilder
{
  pub fn new(file_path : String) -> anyhow::Result<LocalVFileBuilder>
  {
     if File::open(file_path.clone()).is_ok() //we check if the file can be opened without error, to avoid error later
     {
       if let Ok(metadata) = std::fs::metadata(file_path.clone())
       {
         return Ok(LocalVFileBuilder{file_path, size : metadata.len()});
       }
     }
     Err(RustructError::OpenFile(file_path).into()) //return more precise error, like permission error...
  }
}

#[typetag::serde]
impl VFileBuilder for LocalVFileBuilder
{
  fn open(&self) -> anyhow::Result<Box<dyn VFile>>
  {
    match File::open(&self.file_path)
    {
      Ok(file) => {
                    let file = BufReader::new(file);
                    Ok(Box::new(file))
                  },
      Err(_) => Err(RustructError::OpenFile(self.file_path.clone()).into()),
    }
  }

  fn size(&self) -> u64
  { 
    self.size
  }
}
