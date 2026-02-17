//
// API function: get /images
//
// Returns all the images in the database.
//
// Author:
//   Prof. Joe Hummel
//   Northwestern University
//

const mysql2 = require('mysql2/promise');
const { get_dbConn } = require('./helper.js');
//
// p_retry requires the use of a dynamic import:
// const pRetry = require('p-retry');
//
const pRetry = (...args) => import('p-retry').then(({default: pRetry}) => pRetry(...args));


/**
* get_images
:
*
*
@description
returns all the
images
in the database as a JSON object
* {message: ..., data: ...} where message is either "success" or an
* error message
(with status code 500)
. If successful, the data is a list of
*
dictionary
-
like
objects of the form {"
assetid
": int, "
userid
":
int
,
*
"localname": string,
"
bucketkey
": string}, in order by
assetid
. If
an error
*
occurs then the
list is empty [].
If a userid is given, then just the images
*
with that userid are returned; validity of the userid is not checked,
*
which implies that an empty list is returned if the userid is invalid.
*
*
@param
userid (optional
query parameter
) filters the returned images for just
this userid
*
@returns
JSON {message: string, data: [object, object, ...]}
*/
exports.get_images = async (request, response) => {

  async function try_get_images()
  {
    let dbConn = null;
    try {
      //
      // open connection to database:
      //
      dbConn = await get_dbConn();
      await dbConn.beginTransaction();
      let sql = "";
      let params = [];

      if (request.query.userid !== undefined) {
        sql = `
                SELECT assetid, userid, localname, bucketkey
                FROM assets
                WHERE userid = ?
                ORDER BY assetid ASC;
                `;
        params.push(request.query.userid);
      }
      else {
        sql = `
                SELECT assetid, userid, localname, bucketkey
                FROM assets
                ORDER BY assetid ASC;
                `;
      }
      //
      // call MySQL to execute query, await for results:
      //
      console.log("executing SQL...");
      
      let [rows, _] = await dbConn.execute(sql, params);
      await dbConn.commit();
      
      //
      // success, return rows from DB:
      //
      console.log(`done, retrieved ${rows.length} rows`);

      return rows;
    }
    catch (err) {
      //
      // exception:
      //
      console.log("ERROR in try_get_images:");
      console.log(err.message);

        try {
          await dbConn.rollback();
        } catch (rollbackErr) {
          console.log(`rollback error: ${rollbackErr.message}`);
        }

      throw err;  // re-raise exception to trigger retry mechanism

    }
    finally {
      //
      // close connection:
      //
      try { await dbConn.end(); } catch(err) { /*ignore*/ }
    }
  }

  //
  // retry the inner function at most 3 times:
  //
  try {
    console.log("**Call to get /images...");

    let rows = await pRetry(try_get_images, {retries: 2});

    //
    // success, return data in JSON format:
    //
    console.log("success, sending response...");

    response.json({
      "message": "success",
      "data": rows,
    });
  }
  catch (err) {
    //
    // exception:
    //
    console.log("ERROR:");
    console.log(err.message);

    //
    // if an error occurs it's our fault, so use status code
    // of 500 => server-side error:
    //
    response.status(500).json({
      "message": err.message,
      "data": [],
    });
  }

};
