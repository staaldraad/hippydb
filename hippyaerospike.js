var util = require('util')
var aerospike = require("aerospike")

var options = {host:'127.0.0.1',port:3000,limit:10}
exports.commands = ['help','status','info','bins','sets']

var help = function(callback){
       var helpm = "Aerospike dumper -- Help\n"
           helpm += "Commands: \n"
           helpm += "> aerospike status                       //Check if we can connect to the database\n"
           helpm += "> aerospike info                       //list info about the host\n"
           helpm += "> aerospike namespaces                   //get namespaces\n"
           helpm += "> aerospike bins                  //get bins in a namespace\n"
           helpm += "> aerospike sets                        //get sets in a namespace.\n"

       return callback(null,helpm)
}

var getStatus = function(callback)
{
    connector(function(err,client){
         if(err)	 
           return callback(err,'Error connecting to Aerospike')

         return callback(null,'Connected! - Connection is good to go - ')
    })
}
var getInfo = function(callback)
{
        connector(function(err,client)
        {
                  if(err||!client)
                          return callback(err,'Error connecting to Aerospike')
                  else{
                       client.info("namespaces", function(err, response) {
                             var msg = response;
                             client.info("build",function(errr,resp){
                                if(!errr)
                                      msg += resp 
                                return callback(false,msg);
                             })

                       });
                  }
         })
}
var listNamespaces = function(client,callback){
        client.info("namespaces",function(err,response){
                return callback(false,response);
     })
}
var listBins = function(client,namespace,callback)
{
        client.info("bins/"+namespace,function(err,response){
                return callback(false,response);
     })
}
var listSets = function(client,namespace,callback)
{
        client.info("sets/"+namespace,function(err,response){
                return callback(false,response);
     })
}

var dump = function(client,callback)
{
}

var connector = function(callback){
     var client = aerospike.client({hosts:[{addr:options.host,port:options.port}]})
     client.connect(function(response) {
            if ( response.code == aerospike.status.AEROSPIKE_OK ) {
                 return callback(null,client)
            }
            else {
                 return callback(true,'Error connecting to Aerospike')
            }
        });
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
       case 'info':  getInfo(callback); 
                    break;
       case 'namespaces':
                      connector(function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Aerospike')
                         else
                             listNamespaces(con,callback)
                    })
                    break;
       case 'sets':
                      if(!command[2]) 
                              return(callback(true,'Missing namespace name'))
                      connector(function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Aerospike')
                         else
                             listSets(con,command[2],callback)
                    })
                    break;
       case 'bins':
                      if(!command[2]) 
                              return(callback(true,'Missing namespace name'))
                      connector(function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Aerospike')
                         else
                             listBins(con,command[2],callback)
                    })
                    break;
       case 'dump':  
                      connector(function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Redis')
                         else
                             dump(con,callback)
                    })
                    break;
       default: return callback(true,'Command not found');
    }
}
