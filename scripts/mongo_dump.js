/* Quick and dirty scanner of exposed MongoDB instances
   Author: etienne@sensepost.com
   Version: 04/02/2015 
*/

var host = "127.0.0.1:27017";
var d_collections = false;
var dx_collection = false;
var MongoClient = require('mongodb').MongoClient;
var limitsize = 10;

if(process.argv[2])
   host = process.argv[2];

if(process.argv[3]&&process.argv[3]=='c') //if we should extract collections
   d_collections = true;

if(process.argv[3]&&process.argv[3]=='x'){ //we should get values from a specific collection
   //database collection
   dx_collection = true;
   dx_collection_db = process.argv[4]
   dx_collection_name = process.argv[5]
}
 
var url = 'mongodb://'+host+'/';

var getCollections = function(db,callback){
   db.collectionNames(function(err, collections) {
        if(err) 
            return callback(err)
        else
            return callback(null,collections)
   });
}

var getDatabases = function(db,callback){
   db.admin().listDatabases(function(err,dbs){
      if(err){
          return callback(err);
      }
      else 
      {
         return callback(null,dbs);
      }
   });
}

var findDocuments = function(db, callback) {
  var collection = db.collection(dx_collection_name);
  collection.count(function(err,count){  
      collection.find({}).limit(limitsize).toArray(function(err, docs) {
          console.log('[*] Found '+count+' records ('+limitsize+' shown)');
          console.dir(docs)
          callback(docs);
      });      
  });
}


var connector = function(db,callback){
    url_n = url+''+db;
    MongoClient.connect(url_n,function(err,dba){
        if(err)console.error(err)
        return callback(null,dba)
    })

}

if(!dx_collection){
MongoClient.connect(url, function(err, db) {
  if(err){
     console.error(err);
     return;
  }
  console.log("[+] Connected correctly to server");
  console.log("[+] Retrieving databases...");
  
  getDatabases(db,function(err,databases){
     if(err){
         console.error(err)
         db.close();
     }
     else
     {
         console.info(databases) 
         if(d_collections){
            console.info('[+] Extracting collections')
            databases.databases.forEach(function(dba){
                  connector(dba.name,function(err,dd){
                      if(dd){
                          getCollections(dd,function(err,collections){
                               console.log('[*] '+dba.name)
                               collections.forEach(function(col){
                                    console.log(col.name.replace(/^([^.]*)./,""))
                               })
                               console.log('\n')
                               dd.close()
                          });
                      }
                  })     
            })
            db.close()
         }
         else
             db.close()
     }
  })
});
}
if(dx_collection)
{
   console.log('[+] Connecting to database')
   connector(dx_collection_db,function(err,db){
      if(db)
      {
            findDocuments(db,function(err,res){
                db.close() 
            })         
      }
   })
}

