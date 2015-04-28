/*
   connect to and dump keyspace/tables/data from cassandra database
   Usage:
   Get keyspaces: node cassandra_dump.js -h 10.10.0.1
   Get Tables in Keyspace: node cassandra_dump.js -h 10.10.0.1 -k keyspacename
   Get 10 rows from a table: node cassandra_dump.js -h 10.10.0.1 -k keyspacename -t tablename
   Custom SQL/CQL statment: node cassandra_dump.js -h 10.10.0.1 -k keyspacename -s 'SELECT * FROM tablename LIMIT 5'

   Author: etienne@sensepost.com
   Version: 1.0 17 February 2015 
*/

var util = require('util');
var cassandra = require('cassandra-driver');
var argv = require('minimist')(process.argv.slice(2));

if(process.argv.length < 2){
        console.error("Usage: \n --help display this messaage")
        console.error("-h <ip-address> the host to check")
        console.error("-p <port>  the port to use, default 9042")
        console.error("-k <keyspace> dump a list of tables in keyspace or use specific keyspace ")
        console.error("-t <tablename> table to dump (requires -k)")
        console.error("-s <hsql query> run specific hsql query (requires -k)")
        return
}

if(!argv.h)
{
   console.error("Requires at least the host argument. see --help for more")
   return
}

var host = argv.h

if(argv.s){
       var keyspace = argv.k
       var sql = argv.s
       var client = new cassandra.Client({ contactPoints: [host], keyspace: keyspace});
    client.connect(function(err){
        if(err){
            console.error("Shmeh - "+err);
            client.shutdown();
            return;
        }
        console.info("[*] Connected!")
        console.info("[*] Custom SQL command: "+sql);
        client.execute(sql, function (err, result) {
          if(err){
              console.error(err); 
              client.shutdown(); 
              return;
          }
           result.rows.forEach(function(row){ 
                          console.log(util.inspect(row,{showHidden:false,depth:null})) //multiple values
                       // console.log(row.resource_value.toString()); //show one value only (buffer ect)
          })
          client.shutdown();
        });
        
    });

}


if(!argv.k&&!argv.s&&!argv.t){
    var client = new cassandra.Client({ contactPoints: [host], keyspace: "system"});
    client.connect(function(err){
        if(err){
            console.error("Shmeh - "+err);
            client.shutdown();
            return;
        }
        console.info("[*] Connected!")
        console.info("[*] Getting all keyspaces");
        client.execute('SELECT * FROM schema_keyspaces', function (err, result) {
          if(err){
              console.error(err); 
              client.shutdown(); 
              return;
          }
           result.rows.forEach(function(keyspace){ 
              console.log("[+] Found keyspace: "+keyspace.keyspace_name);
          })
          client.shutdown();
        });
        
    });
}
if(argv.k&&!argv.t){
    var keyspace = argv.k
    var client = new cassandra.Client({ contactPoints: [host], keyspace: "system"});
    client.connect(function(err){
         if(err){console.error(err);return;}
         console.info('[*] Getting columnfamilies for: '+keyspace);
         client.execute('SELECT * FROM schema_columnfamilies WHERE keyspace_name = ?',[keyspace], function(err,result){
             if(err){
                  console.error(err);
                  console.error("[*] giving you everything we can... ")
                  client.execute('SELECT * FROM schema_columnfamilies', function(err,results){
                      results.rows.forEach(function(row){
                             console.log(row.keyspace_name+' : '+row.columnfamily_name);
                      });
                      client.shutdown();
                      return;
                  })
             }
             else{
             result.rows.forEach(function(row){
                console.log(row.columnfamily_name);
             });
             client.shutdown();
             }
        });
    })
}

if(argv.k&&argv.t)
{
    var keyspace = argv.k
    var table = argv.t
    var client = new cassandra.Client({ contactPoints: [host], keyspace: keyspace});
    client.connect(function(err){
         if(err){console.error(err);return;}
         console.info('[*] Getting values (10) for: '+keyspace+'.'+table);
         client.execute('SELECT * FROM '+table+' LIMIT 10', function(err,result){
             if(err){console.error(err);client.shutdown();return;}
             result.rows.forEach(function(row){
                    console.log(util.inspect(row,{showHidden:false,depth:null}));
             });
             client.shutdown();
        });
    })

}

