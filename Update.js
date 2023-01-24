const dotenv = require('dotenv')
const { config } = require('process')
const fetch = require('sync-fetch')
const { constants } = require('buffer')
const { OPCUAClient, makeBrowsePath, SecurityPolicy, MessageSecurityMode, AttributeIds, resolveNodeId, TimestampsToReturn} = require("node-opcua");
const async = require("async");
const { time } = require('console')
const mqtt = require('mqtt');
const { mixin } = require('lodash');
const { type } = require('os');
const fetchp = require('node-fetch')


dotenv.config({ path: './config.properties' })

let run_once = true

console.log(process.env.CONFIG_URL_PREFIX)

function typeTransform(data){
  console.log("++++")
  console.log(data)
  console.log("----")
  if (data===true){
    return 1
  } else if (data===false){
    return 0
  } else {
    return data.toFixed(5)
  }
}
function notHaveInitVariables(dict){
    if((dict.MQTT_URL=="") || (dict.CONFIG_URL_PREFIX=="")) {
        return 1
    } 
    return 0
}

function notHaveConfigVariables(dict){
  console.log(dict.TAG_PREFIX)
  console.log(dict.taglist)
  console.log(dict.SUBSCRIBE_INTERVAL)
    // if (!dict.OPC_SERVER_USER || !(dict.PROG_ID_PREFER).toString || !dict.taglist || !dict.OPC_SERVER_HOST || (!dict.OPC_SERVER_CLSID && !dict.OPC_SERVER_PROGID)) {
    if (!dict.TAG_PREFIX  || !dict.taglist || !dict.SUBSCRIBE_INTERVAL ) {
        return 1
    }
    return 0
}

function getConfig(dict){
    let URI = dict.CONFIG_URL_PREFIX + "/clients/" + dict.CLIENT_ID + "/ingestconfigs/"+ dict.CONFIG_ID
    console.log(URI)
    const config = fetch(URI, {
        headers: glob_headers
    }).json()

    URI = dict.CONFIG_URL_PREFIX + "/ingestconfigs/"+ dict.CONFIG_ID + "/tags"
    console.log(URI)
    const tags = fetch(URI, {
        headers: glob_headers
    }).json()

    // config.taglist = tags.map(tag => tag["dataTagId"])
    config.taglist = tags
    return config
}

function setStatus(dict){

    let URI = dict.CONFIG_URL_PREFIX  + "/ingestconfigs/"+ dict.CONFIG_ID + "/statuses"
    console.log(URI)
    const status = fetch(URI, {
        headers: glob_headers
    }).json()
    
    // console.log(dict)
    URI = dict.CONFIG_URL_PREFIX + '/statuses/update?where={"id":"'+status["id"]+'"}'

    console.log(URI)
    const metadata = fetch(URI, {
        method: "POST",
        body: JSON.stringify({"status": process.env.STATUS || 0, "time": (+new Date() + 2*19800), "cached": false }),
        headers: glob_headers
    })
    console.log(metadata.status)
    if (metadata.status==200){
        return 1
    }
    return 0
}

//
function authenticate(){
  return fetchp(process.env.CONFIG_URL_PREFIX+"/Users/login", {
    "headers": {
      "content-type": "application/json;charset=UTF-8",
      "Accept":"application/json"
    },
    "body": "{\"email\":\""+process.env.API_AUTH_USERNAME+"\",\"password\":\""+process.env.API_AUTH_PASSWORD+"\"}",
    "method": "POST"
  })
}

//------------------main program-----------------------

// New flow for authentication

let glob_headers = {
  "Content-Type": "application/json",
  "Accept":"application/json",
  "Authorization": ""
}

if (notHaveInitVariables(process.env)){
    console.error("Config.properties incomplete")
    process.exit()
}


fetchp(process.env.CONFIG_URL_PREFIX.replace("/exactapi", "/opc-network"), {
  "headers": {
    "withCredentials": true
  }
})
.then(function(response){
  if (!response.ok) {
    return authenticate()
  }
  return response
})
.then(response=> response.json())
.then(function(data){

  try{
    glob_headers["Authorization"] = data.id
    console.log(data.id)
    console.log("Authenticated")
  } catch(e){
  }

  if (process.argv.length>2){
      process.env.CLIENT_ID = process.argv[2]
      process.env.CONFIG_ID = process.argv[3]
      process.env.STATUS = 1
      let testmode = false;


      if (process.env.CLIENT_ID == "test" || process.env.CLIENT_ID == "TEST"){
          let testmode = true;
          process.env.OPC_SERVER_HOST=process.argv[4]

          // process.env.OPC_SERVER_PORT=process.argv[4]
          // process.env.OPC_SERVER_PATH=process.argv[5]
        process.env.TAG_PREFIX=process.argv[5]
          process.env.SUBSCRIBE_INTERVAL=process.argv[6]
        process.env.taglist ='["'+process.argv[7]+'"]'
        //console.log(process.env.taglist)
          process.env.OPC_SERVER_CLSID=process.argv[8]
          process.env.OPC_SERVER_USER=process.argv[9]
          process.env.OPC_SERVER_PASS=process.argv[10]
      } else{

          // setStatus(process.env)
          process.env.STATUS = 0
          if(!setStatus(process.env)){
              console.error("STATUS not initiated, retry START")
              process.exit()
              console.error("igonoreing exit")
          }

          // MOVES only if able to set status (handled at OPC client side to create status if not)
          let config = getConfig(process.env)

          if (config.length<1){
              console.error("Config not created at cloud")
              process.exit()
          }
          if (notHaveConfigVariables(config)){
              console.error("Config incomplete, exitting.")
              process.exit()
          }

          // Moves only if config created at cloud
          for (param in config){
            if (typeof(config[param])=="object"){
              process.env[param] = JSON.stringify(config[param])
            } else{
              process.env[param] = config[param].toString()
            }
          }
      }

  } else{ 
      console.error("Run time arguments incomplete")
      process.exit()
  }

     
  //config available
  console.log("**********Initializing OPC Client**********")


  const nodeId = process.env.OPC_ROOT_NODE;

  // const endpointUrl = "opc.tcp://" + process.env.OPC_SERVER_HOST + ":"+process.env.OPC_SERVER_PORT+process.env.OPC_SERVER_PATH;
  const endpointUrl = process.env.OPC_SERVER_HOST;

  // if (process.env.OPC_SERVER_PROGID)
  const client = OPCUAClient.create({
    "endpoint_must_exist": false
    //"securityPolicy": SecurityPolicy.Basic256,
    //"securityMode": MessageSecurityMode.SignAndEncrypt
  });

  console.log("**********Attempting to connect to OPC**********")


  let the_session, the_subscription;
  // const MQclient  = mqtt.connect(process.env.MQTT_URL)
  const MQclient = mqtt.connect(process.env.MQTT_URL, { 
    keepalive: 120 , 
    username: process.env.MQTT_AUTH_USERNAME, 
    password: process.env.MQTT_AUTH_PASSWORD
  });

  MQclient.on('connect', function () {
            console.log("**********Connected to BROKER**********");
            //callback();
          })
      async.series([
      function(callback){
          callback();

      },

      function(callback)  {
        client.connect(endpointUrl, function(err) {
          if (err) {
            console.log("---------cannot connect to endpoint------------:", endpointUrl);
          } else {
            console.log("**********Successfully Connected to OPC-UA**********");
          }
          callback(err);
        });
      },

      function(callback) {
        if (process.env.OPC_SERVER_PASS){
          client.createSession({"userName": process.env.OPC_SERVER_USER, "password":process.env.OPC_SERVER_PASS}, function(err, session) {
            if (err) {
              return callback(err);
            }
            the_session = session;
            callback();
          });
        } else{
          client.createSession(function(err, session) {
            if (err) {
              return callback(err);
            }
            the_session = session;
            callback();
          });
        }
        
      },
      function(callback) {
          
        const subscriptionOptions = {
          maxNotificationsPerPublish: 5000,
          publishingEnabled: true,
          requestedLifetimeCount: 100,
          requestedMaxKeepAliveCount: 10
          // requestedPublishingInterval: 60000
          // publishingInterval: 60000
        };
        the_session.createSubscription2(subscriptionOptions, (err, subscription) => {
          if (err) {
            return callback(err);
          }
        
          the_subscription = subscription;
      
          callback();
        });
      },
      function(callback) {
        console.log("Susbcribe interval", process.env.SUBSCRIBE_INTERVAL)
        const monitoringParamaters = {
          samplingInterval: parseInt(process.env.SUBSCRIBE_INTERVAL),
          discardOldest: true,
          queueSize: 10
        };
      
        let itemsToMonitor = []
        tags = JSON.parse(process.env.taglist)
        console.log(tags)

        let access_way;
        console.log("access way")

        tags.forEach(function(tag){
          if (process.env.PROG_ID_PREFER){
              try {
                if (tag["address"]){
                  tag = tag["address"]
                  access_way = ";i="
                } else {
                  tag = tag["dataTagId"]
                  access_way = ";s=1:"
                }
                
              } catch (error) {
                console.log(error)
                  access_way = ";s=1:"
                
              }
          } else{
            tag = tag["dataTagId"]
            access_way= ";s=1:"
          }

          let temp = {
            attributeId: AttributeIds.Value,
            nodeId: "ns="+process.env.OPC_SERVER_CLSID+access_way+tag+"?Value"
          }     
          console.log(temp)
          itemsToMonitor.push(temp)
        })

        let status_count = 0
        console.log("items to monitor----------")
        console.log(itemsToMonitor)
        console.log(the_subscription)
        const obj=new map();
        the_subscription.monitorItems(
            itemsToMonitor,
            monitoringParamaters,
            TimestampsToReturn.Both, function(err, mItems){
              if (err){
                console.log("error in monitoring items")
                console.log(err)
              }
              mItems.on("changed", function (monitoredItem, dataValue, index){
                    console.log(" The value has changed : ",monitoredItem, dataValue);  
                    //let ts = +new Date(dataValue.sourceTimestamp.toString());
                    let ts = +new Date();
                    let topic_line = process.env.CLIENT_ID + "/" + process.env.CONFIG_ID + "/"+process.env.TAG_PREFIX + tags[index].dataTagId
                    obj.set(process.env.TAG_PREFIX,dataValue.value);
                    publish(process.env.TAG_PREFIX);
                    console.log(topic_line)
                    obj.set(process.env.TAG_PREFIX,dataValue.value);
                    if (status_count==0){
                      setStatus(process.env)
                    }

                    status_count = status_count+1
                    status_count = status_count % 500
                    try{
                      let payload = JSON.stringify({"v": typeTransform(dataValue.value.value), "t": ts})
                      MQclient.publish(topic_line, payload)
                      console.log(ts)
                      console.log("--------------------")
                    } catch(e){
                      console.log(e)
                      return
                    }
                    if (process.env.CLIENT_ID == "test" || process.env.CLIENT_ID == "TEST"){
                      setTimeout(()=>process.exit(), 5000);                       
                    }

                });
            })
        
        // If a data change is detected print the value
        // 

        // multi subs
      
        console.log("-----------------Subscription Added--------------------");
      process.env.STATUS = 1
      
      // setStatus(process.env)

        setInterval(()=>{
          fetchp(process.env.CONFIG_URL_PREFIX.replace("/exactapi", "/opc-network"), {
            "headers": {
              "withCredentials": true
            }
          })
          .then(function(response){
            if (!response.ok) {
              return authenticate()
            }
            return response
          })
          .then(response=> response.json())
          .then(function(data){

            try{
              glob_headers["Authorization"] = data.id
              console.log(data.id)
              console.log("Authenticated")
            } catch(e){
            }
          })

        }, 300000)

      },
      function(callback) {
          //to keep event loop on
        console.log("+++++++++++++++++++++++++++++++++++")
          //setTimeout(()=>callback(), 999999); 
      }
    

  ],
  function(err) {
      if (err) {
          console.log(" failure ",err);
      } else {
          console.log("done!");
      }
      client.disconnect(function(){});
  });
  setInterval(publish, 60000);
  function publish() {
  for (const x of obj.values()) {
        try{
            let payload = JSON.stringify({"v": typeTransform(x), "t": ts})
            MQclient.publish(topic_line, payload)
            console.log(ts)
            console.log("--------------------")
            } catch(e){
            console.log(e)
            return
            }
         }
    } 
})
