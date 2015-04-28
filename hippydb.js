
var mongo = require('./hippymongodb')
var cassandra = require('./hippycassandra')
var redis = require('./hippyredis')
var riak = require('./hippyriak')
var memcached = require('./hippymemcached')
var hbase = require('./hbase/hippyhbase')
var hive = require('./hive/hippyhive')

function completer(line) {
  var completions = 'help exit quit q list info mongodb cassandra riak redis memcached'.split(' ')
  var hits = completions.filter(function(c) { return c.indexOf(line) == 0 })
  return [hits.length ? hits : completions, line]
}

function log(type,line)
{
   switch(type){
       case 'error':
                    console.log("\033[91m[x]\033[0m %s",line) 
                    break
       case 'warn':
                    console.log("\033[93m[-]\033[0m %s",line) 
                    break
       case 'info':
                   console.log("\033[92m[*]\033[0m %s",line)
                   break;
       default:    
                   console.log(type)
                   break;
   }
}

var options = {host:'127.0.0.1',port:'',verbose:false,limit:10} //default options
var dbs = ['cassandra','hbase','hive','memcached','mongodb','redis','riak'] //supported databases

var readline = require('readline'),
    rl = readline.createInterface(process.stdin, process.stdout,completer);

log('info',' Hippy Database interaction tool.')
log('info',' Author: etienne@sensepost.com\n')
log('info',' Type \033[92mhelp\033[0m to get started.')

rl.setPrompt('> ');
rl.prompt();

rl.on('line', function(line) {
  var l = line.trim()
  var l_args = []
  if(l.split(' ').length > 1)
  {
      l_args = l.split(' ')
      l = l_args[0]
  }
  if(dbs.indexOf(l) > -1){//db specific command
      rl.pause()
      switch(l){
          case 'cassandra':
                           cassandra.setOptions({'host':options.host,'limit':options.limit,'port':options.port?options.port:null})
                           cassandra.commandParse(l_args,function(err,message){
                              if(err){ 
                                  log('error',message)
                              }
                              else{
                                  if(message)
                                     log('info',message)
                              }
                              rl.prompt()
                              rl.resume()
                          });
                         break;
          case 'couchdb':break;
          case 'hbase':
                           hbase.setOptions({'host':options.host,'limit':options.limit,'port':options.port?options.port:null})
                           hbase.commandParse(l_args,function(err,message){
                              if(err){ 
                                  log('error',message)
                              }
                              else{
                                  if(message)
                                     log('info',message)
                              }
                              rl.prompt()
                              rl.resume()
                          });
                        break;
          case 'hive':
                           hive.setOptions({'host':options.host,'limit':options.limit,'port':options.port?options.port:null})
                           hive.commandParse(l_args,function(err,message){
                              if(err){ 
                                  log('error',message)
                              }
                              else{
                                  if(message)
                                     log('info',message)
                              }
                              rl.prompt()
                              rl.resume()
                          });
                         break;
          case 'memcached':
                          memcached.setOptions({'host':options.host,'limit':options.limit,'port':options.port?options.port:null})
                          memcached.commandParse(l_args,function(err,message){
                              if(err){ 
                                  log('error',message)
                              }
                              else{
                                  if(message)
                                     log('info',message)
                              }
                              rl.prompt()
                              rl.resume()
                          });
                          break;
          case 'mongodb': 
                          mongo.setOptions({'host':options.host,'limit':options.limit,'port':options.port?options.port:null})
                          mongo.commandParse(l_args,function(err,message){
                              if(err){ 
                                  log('error',message)
                              }
                              else{
                                  if(message)
                                     log('info',message)
                              }
                              rl.prompt()
                              rl.resume()
                          });
                          break;
          case 'redis':
                          redis.setOptions({'host':options.host,'limit':options.limit,'port':options.port?options.port:null})
                          redis.commandParse(l_args,function(err,message){
                              if(err){ 
                                  log('error',message)
                              }
                              else{
                                  if(message)
                                     log('info',message)
                              }
                              rl.prompt()
                              rl.resume()
                          });
			break;
          case 'riak':
                          riak.setOptions({'host':options.host,'limit':options.limit,'port':options.port?options.port:null})
                          riak.commandParse(l_args,function(err,message){
                              if(err){ 
                                  log('error',message)
                              }
                              else{
                                  if(message)
                                     log('info',message)
                              }
                              rl.prompt()
                              rl.resume()
                          });
			break;
      }
  }
  else{ //we have a 'standard' command. Not db specific
	  switch(l) {
	    case 'exit':
	    case 'quit':
	    case 'q':
		       console.log('Cheerio old-chap');
		       process.exit(0);
		       break;
	    case 'help':
	    case '?':
		       log('info','help exit quit q list info')
		       break;    
            case 'info':
                       log('info','Default ports for supported databases')
                       log('6379/tcp  -- Redis')
                       log('8087/tcp  -- Riak')
                       log('9042/tcp  -- Cassandra')
                       log('9090/tcp  -- HBase')
                       log('11211/tcp -- Memcached')
                       log('27017/tcp -- MongoDB')
                       log('10000/tcp -- Hive')
                       break;
	    case 'options':
		       log('info',options) 
		       break;
	    case 'list':
		       log('info','Supported databases: ')
                       dbs.forEach(function(db){
                             log(db)
                       }) 
		       break;
	    case 'set':
		       if(l_args.length < 3){
			       log('warn','Not enough arguments. Try: set option value')
			       break
		       }
                       if(!isNaN(l_args[2]))
		           options[l_args[1]] = parseInt(l_args[2])
                       else
		           options[l_args[1]] = l_args[2]
		       log('info',l_args[1]+'='+options[l_args[1]])
		       break;
	    default:
	      log('warn','Command not recognised. Try \'help\'');
	      break;
	  }
        rl.prompt()
  }

}).on('close', function() {
  console.log('Cheerio old-chap')
  process.exit(0);
});
