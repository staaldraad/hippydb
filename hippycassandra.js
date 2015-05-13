var util = require('util');
var cassandra = require('cassandra-driver');

var options = {host:'127.0.0.1',port:9042,limit:10}

var help = function(callback){
       var helpm = "Cassandra DB dumper -- Help\n"
           helpm += "Commands: dbs, collections, docs, status\n"
           helpm += "> cassandra status                        //Check if we can connect to the database\n"
           helpm += "> cassandra keys                          //this will list all databases on the host\n"
           helpm += "> cassandra tables <keyspace-name>        //show all collections in a database\n"
           helpm += "> cassandra dump   <keyspace-name.table>  //show 'x' number of documents in the db-name.collection.\n"
           helpm += "                                          //use keyspace-name.table'\n"
           helpm += "> cassandra cql 'statement'               //run a specific CQL statement like: 'SELECT * FROM tablename LIMIT 5'\n"

       return callback(null,helpm)
}

var getStatus = function(callback)
{
    var stat = ""
    connector('system',function(err,con){
	 if(err)
	     return callback(err,'Error connecting to Cassandra')
	 else
             return callback(null,'Connected! - Connection is good to go')
    })
}

var connector = function(keyspace,callback){
    var client = new cassandra.Client({ contactPoints: [options.host], keyspace:keyspace});
    client.connect(function(err){
        if(err){
            return callback(err,'Error connecting to Cassandra');
        }
        return callback(null,client)
    })

}

var listKeyspaces = function(client,callback){
      client.execute('SELECT * FROM schema_keyspaces', function (err, result) {
          if(err){
              client.shutdown(); 
              return callback(err,err);
          }
           var results = "Found ["+result.rows.length+"] keyspaces\n"
           result.rows.forEach(function(keyspace){ 
              results += keyspace.keyspace_name+"\n";
          })
          client.shutdown();
          return callback(null,results)
          
        });
}

var listTables = function(client,keyspace,callback)
{
    var message = ''
    client.connect(function(err){
         if(err){
              return callback(err,err)
         }
         client.execute('SELECT * FROM schema_columnfamilies WHERE keyspace_name = ?',[keyspace], function(err,result){
             if(err){
                  message = "Got an error, giving you everything we can... \n"
                  client.execute('SELECT * FROM schema_columnfamilies', function(err,results){
                      if(!results||err){
                          return callback(true,err);
                      }
                      results.rows.forEach(function(row){
                             message += row.keyspace_name+' : '+row.columnfamily_name+'\n';
                      });
                      client.shutdown();
                      return callback(null,message);
                  })
             }
             else{
                message = "Found ["+result.rows.length+"] tables/columnfamilies\n"
                result.rows.forEach(function(row){
                    message += row.columnfamily_name+'\n';
                });
                client.shutdown();
                return callback(null,message)
             }
        });
    })
}

var dumpValues = function(client,table,callback){
    client.connect(function(err){
         if(err){
             return callback(err,err)}
         var message = 'Getting ['+options.limit+'] values\n';
         client.execute('SELECT * FROM '+table+' LIMIT '+options.limit, function(err,result){
             if(err){
                 client.shutdown();return callback(err,err)
             }
             result.rows.forEach(function(row){
                    message += util.inspect(row,{showHidden:false,depth:null})+'\n';
             });
             client.shutdown();
             return callback(null,message)
        });
    })


}

var customCQL = function(client,cql,callback)
{
     client.connect(function(err){
        if(err){
            client.shutdown();
            return callback(err,err)
        }
        var message = "Executing Custom CQL command: "+cql+"\n";
        client.execute(cql, function (err, result) {
          if(err){
              client.shutdown(); 
              return callback(err,err)
          }
           message += "Found ["+result.rows.length+"] values\n"
           result.rows.forEach(function(row){ 
                 message += util.inspect(row,{showHidden:false,depth:null})+'\n' //multiple values
                       // console.log(row.resource_value.toString()); //show one value only (buffer ect)
          })
          client.shutdown();
          return callback(null,message)
        });
        
    });

}

exports.setOptions=function(noptions){
      options.host = noptions.host? noptions.host:options.host
      options.port = noptions.port? noptions.port:options.port
      options.limit = noptions.limit? noptions.limit:options.limit
}

exports.commandParse=function(command,callback)
{
    switch(command[1]){
       case 'help': 
                    help(callback); 
		    break;
       case 'status': 
                     getStatus(callback);
                      break;
       case 'keys':  connector('system',function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Cassandra')
                         else
                             listKeyspaces(con,callback)
                    })
                    break;
       case 'tables':
                      if(!command[2]) 
                              return(callback(true,'Missing keyspace name'))
                      connector('system',function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Cassandra')
                         else
                             listTables(con,command[2],callback)
                    })
                    break;
       case 'dump':  
                      if(!command[2]) 
                             return callback(true,'Missing database and table name')
                      var tmp = command[2].split('.',2)
                      var keyspace = tmp[0], tbl = tmp[1]
                      if(!tbl)
                             return callback(true,'Missing table name')
                      connector(keyspace,function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Cassandra')
                         else
                             dumpValues(con,tbl,callback)
                    })
                    break;
       case 'cql':   if(command.length<3) 
                             return callback(true,'Missing keyspace and CQL string')
                      var keyspace = command[2].split(' ',1)
                      var cql = ''
                      for(var i=3; i<command.length; i++)
                          cql += command[i]+' '
                      cql = cql.substring(1,cql.length-2).trim()
                      if(!cql)
                             return callback(true,'Missing CQL name')
                      connector(keyspace,function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Cassandra')
                         else
                             customCQL(con,cql,callback)
                    })

                    break;
       default: return callback(true,'Command not found');
    }
}
