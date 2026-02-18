//
// PhotoApp web service based on Node.js and Express. This file
// contains the main function that starts and listens on the
// configured network port. The remaining API functions are 
// defined in separate JS files for easier development.
//
// Authors:
//  Alec do Couto
//  Prof. Joe Hummel (initial template)
//  Northwestern University
//
// References:
// Node.js: 
//   https://nodejs.org/
// Express: 
//   https://expressjs.com/
// MySQL2: 
//  https://sidorares.github.io/node-mysql2/docs
// AWS SDK with JS:
//   https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/index.html
//   https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/getting-started-nodejs.html
//   https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-s3/
//   https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/javascript_s3_code_examples.html
//

const express = require('express');
const app = express();
const config = require('./config.js');

// support larger image uploads/downloads:
app.use(express.json({ strict: false, limit: "50mb" }));


/**
 * main:
 *
 * @description startup code for web service, starts listening on port
 * @param none
 * @returns none
 */
var startTime;

app.listen(config.web_service_port, () => {
  startTime = Date.now();
  console.log(`**Web service running, listening on port ${config.web_service_port}...`);
  //
  // Configure AWS to use our config file:
  //
  process.env.AWS_SHARED_CREDENTIALS_FILE = config.photoapp_config_filename;
});


/**
 * get /
 * 
 * @description handles request for what would be default page as if
 * we were a web server. Returns startus and how long the service has
 * been up and running (seconds).
 * 
 * @param request
 * @param response
 * @returns {status: string, uptime_in_seconds: integer}
 */
app.get('/', (request, response) => {
  try {
    console.log("**Call to /...");
    
    let uptime = Math.round((Date.now() - startTime) / 1000);

    console.log("sending response...");

    response.json({
      "status": "running",
      "uptime_in_secs": uptime,
    });
  }
  catch(err) {
    console.log("ERROR:");
    console.log(err.message);

    //
    // if something goes wrong it's our fault, ==> use a
    // status code of 500 ==> server-side error:
    //
    response.status(500).json({
      "status": err.message,
      "uptime_in_secs": uptime,
    });
  }
});


//
// web service API functions, one per JS file:
//
//
// 1. app.get('/ping', async (request, response) => {...});
//
let get_ping_file = require('./api_get_ping.js');
app.get('/ping', get_ping_file.get_ping);
//
// 2. app.get('/users', async (request, response) => {...});
//
let get_users_file = require('./api_get_users.js');
app.get('/users', get_users_file.get_users);

//
// load and define more API functions here:
//
let get_images_file = require('./api_get_images.js');
app.get('/images', get_images_file.get_images);

let get_image_file = require('./api_get_image.js');
app.get('/image/:assetid', get_image_file.get_image);

let post_image_file = require('./api_post_image.js');
app.post('/image/:userid', post_image_file.post_image);

let get_image_labels = require('./api_get_image_labels.js');
app.get('/image_labels/:assetid', get_image_labels.get_image_labels); 

let get_images_with_label = require('./api_get_images_with_label.js');
app.get('/images_with_label/:label', get_images_with_label.get_images_with_label);

let delete_images = require('./api_delete_images.js');
app.delete('/images', delete_images.delete_images);
