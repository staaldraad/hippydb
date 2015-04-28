var util = require('util')
var MongoClient = require('mongodb').MongoClient 

//defaults
var options = {host:'127.0.0.1',port:'27017',limit:10}

var help = function(callback){
       var helpm = "MongoDB dumper -- Help\n"
           helpm += "Commands: dbs, collections, docs, status\n"
           helpm += "> mongodb status                     //Check if we can connect to the database\n"
           helpm += "> mongodb dbs                        //this will list all databases on the host\n"
           helpm += "> mongodb collections <db-name>      //show all collections in a database\n"
           helpm += "> mongodb dump <db-name.collection>  //show 'x' number of documents in the db-name.collection.\n"
           helpm += "                                     //use db-name.collection, as found using 'mongodb collections'\n"

       return callback(null,helpm)
}

var getStatus = function(callback)
{
    var stat = ""
    connector('',function(err,con){
	 if(err)
	     return callback(err,'Error connecting to MongoDb')
	 else
             return callback(null,'Connected! - Connection is good to go')
    })
}
var listCollections = function(db,callback){
   db.collectionNames(function(err, collections) {
        if(err) 
            return callback(err,'')
        else{
            var colNames = 'Found '+collections.length+' collections\n'
            collections.forEach(function(col){
                 colNames += col.name+'\n'
            })
            return callback(null,colNames)
        }
   });
}

var listDatabases = function(db,callback){
   db.admin().listDatabases(function(err,dbs){
      if(err){
          return callback(err,err);
      }
      else 
      {
         var message = 'Found '+dbs.databases.length+' databases\n'
         dbs.databases.forEach(function(db){
             message += db.name+'\n'
         })
         return callback(null,message);
      }
   });
}

var listDocuments = function(db,collection, callback) {
  var collection = db.collection(collection);
  collection.count(function(err,count){  
          if(err) 
                return callback(err,'Error occurred fetching collection')
	  collection.find({}).limit(options.limit).toArray(function(err, docs) {
              if(err) 
                    return callback(err,'Error occurred fetching collection')
              var message = 'Found '+count+' records ('+options.limit+' shown)\n';
              docs.forEach(function(doc){
                    message += util.inspect(doc,{showHidden:false,depth:null})+'\n';
              })
	      return callback(null,message);
	  });      
  });
}


var connector = function(db,callback){
    console.log('[*] Busy...')
    url_n = 'mongodb://'+options.host+'/'+db;
    MongoClient.connect(url_n,function(err,dba){
        if(err)
             return callback(err,null)
        else
             return callback(null,dba)
    })
}

exports.setOptions=function(noptions){
      options.host = noptions.host? noptions.host:options.host
      options.port = noptions.port? noptions.port:options.port
      options.limit = noptions.limit? noptions.limit:options.limit
}

exports.commandParse=function(command,callback)
{
    var commands = ['help','collections','dbs','docs','']
    switch(command[1]){
       case 'help': 
                    help(callback); 
		    break;
       case 'status': 
                     getStatus(callback);
                      break;
       case 'dbs':  connector('',function(err,con){
                         if(err)
                             return callback(err,'Error connecting to MongoDb')
                         else
                             listDatabases(con,callback)
                    })
                    break;
       case 'collections':
                      if(!command[2]) 
                              return(callback(true,'Missing database name'))
                      connector(command[2],function(err,con){
                         if(err)
                             return callback(err,'Error connecting to MongoDb')
                         else
                             listCollections(con,callback)
                    })
                    break;
       case 'dump':  
                      if(!command[2]) 
                             return callback(true,'Missing database and collection name')
                      var tmp = command[2].split('.',2)
                      var db = tmp[0], col = tmp[1]
                      if(!col)
                             return callback(true,'Missing collection name')
                      console.log(db,col)
                      connector(db,function(err,con){
                         if(err)
                             return callback(err,'Error connecting to MongoDb')
                         else
                             listDocuments(con,col,callback)
                    })
                    break;
       default: return callback(true,'Command not found');
    }
}
