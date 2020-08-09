import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
//import * as stripe from "stripe";

admin.initializeApp({
  credential: admin.credential.applicationDefault(),
  databaseURL: "https://mifo-33ee4.firebaseio.com/",
  storageBucket: "mifo-33ee4.appspot.com",
});

const db = admin.firestore();

const { BigQuery } = require("@google-cloud/bigquery");

const fs = require("fs-extra");
const path = require("path");
const os = require("os");

const { Parser } = require("json2csv");
const cors = require("cors")({
  origin: true,
});

enum USER_ROLE {
  OPERATOR = 1,
  DEV_ADMIN = 2,
  DEV_OPERATOR,
}

enum USER_STATUS {
  PENDING = 0,
  VERIFED,
  DISABLED,
}

exports.UserCreated = functions.auth.user().onCreate(async (user) => {
  const firebaseUID = user.uid;
  const email: string = user.email as string;
  const domain = email.slice(email.indexOf("@"));
  const domainData = await db
    .collection(`Domaines`)
    .where("domain", "==", domain)
    .get();

  if (domainData.empty === true) {
    await db.collection("Domaines").add({
      domain: domain,
      admin: [firebaseUID],
    });

    return db.doc(`users/${firebaseUID}`).update({
      role: USER_ROLE.DEV_OPERATOR,
      user_state: USER_STATUS.PENDING,
    });
  }

  if (domain === "tenmas") {
    return db.doc(`users/${firebaseUID}`).update({
      role: USER_ROLE.OPERATOR,
      user_state: USER_STATUS.PENDING,
    });
  }

  return db.doc(`users/${firebaseUID}`).update({
    role: USER_ROLE.DEV_ADMIN,
    user_state: USER_STATUS.PENDING,
  });
});

// interface IUser {
//   name : string;
//   email: string;
// }

exports.UserCreation = functions.firestore
  .document("users/{id}")
  .onCreate((snap, context) => {
    const id = context.params.id;
    const data: any = snap.data();
    console.log(data);

    const promises: any[] = [];

    admin
      .auth()
      .getUserByEmail(data.email)
      .then((user) => {
        if (user === null) {
          const user_state = admin.auth().createUser({
            email: data.email,
            displayName: data.name,
            password: "tenmas",
            emailVerified: false,
          });
          promises.push(user_state);
        }
        return;
      })
      .catch((e) => {
        console.error(e);
      });

    return Promise.all(promises)
      .then((message: any[]) => {
        return db.collection(`users`).doc(id).update({
          user: message[0].uid,
        });
      })
      .catch((e: Error) => {
        console.log(e);
      });
  });

exports.CreditCreation = functions.firestore
  .document("credits/{id}")
  .onCreate((snap, context) => {
    const id = context.params.id;
    const data: any = snap.data();
    console.log(data);

    const credit_update = admin
      .firestore()
      .collection(`credits`)
      .doc(`${id}`)
      .update({
        phases: new Array(11).fill({
          documents: null,
          is_completed: false,
        }),
        domain: ["tenmas", ...data.domain],
        status: 0,
      });
    const notify = admin
      .firestore()
      .collection(`notifications`)
      .add({
        domain: ["tenmas", ...data.domain],
        owner: data.owner,
        operator: "",
        viewed_by_owner: false,
        viewed_by_admin: false,
        viewed_by_operator: false,
        viewed_by_super_admin: false,
        priority: 0,
        options: {
          credit_id: id,
        },
        createdAt: admin.firestore.Timestamp.now(),
      });

    const query = insertCredit(data.domain[0], "credit", data);

    const promises: any[] = [credit_update, notify, query];

    return Promise.all(promises)
      .then((message: any) => {
        console.log(message);
      })
      .catch((e: Error) => {
        console.error(e);
      });
  });

exports.CreditUpdate = functions.firestore
  .document("credits/{id}")
  .onUpdate((snap, context) => {
    const id = context.params.id;
    const beforeDoc = snap.before.data();
    const afterDoc = snap.after.data();

    if (beforeDoc.status === afterDoc.status) {
      return;
    }
    const promises: any[] = [];
    
    if (afterDoc.status === 3) {
      const notify = admin
        .firestore()
        .collection(`notifications`)
        .add({
          domain: ["tenmas", ...afterDoc.domain],
          owner: afterDoc.owner,
          operator: afterDoc.operator,
          viewed_by_owner: false,
          viewed_by_admin: false,
          viewed_by_operator: false,
          viewed_by_super_admin: false,
          priority: 0,
          options: {
            credit_id: id,
          },
          createdAt: admin.firestore.Timestamp.now(),
        });
      promises.push(notify);
    }

    const query = updateCredit(afterDoc.domain[0], "credit", snap.after.data());
    promises.push(query);

    return Promise.all(promises)
      .then((message: any) => {
        console.log(message);
      })
      .catch((e: Error) => {
        console.error(e);
      });
  });

// Notify when Assigned to operator

export const createCSV = functions.firestore
  .document("reports/{reportId}")
  .onCreate((snap, context) => {
    // Step 1. Set main variables

    const reportId = context.params.reportId;
    const fileName = `reports/${reportId}.csv`;
    const tempFilePath = path.join(os.tmpdir(), fileName);

    // Reference report in Firestore
    const reportRef = db.collection("reports").doc(reportId);

    // Reference Storage Bucket
    const storage = admin.storage().bucket(); // or set to env variable

    // Step 2. Query collection
    return db
      .collection("credits")
      .get()
      .then((querySnapshot) => {
        /// Step 3. Creates CSV file from with orders collection
        const credits: any[] = [];

        // create array of users
        let idx = 0;
        querySnapshot.forEach((doc) => {
          credits.push({
            "No.": idx,
            EJECUTIVO: doc.data().tenmas_operator.name,
            PROMOTOR: doc.data().domain[1],
            ASESOR: JSON.parse(doc.data().dev_operator).name,
            "RECEP. EXP. POR TEN MAS": doc.data().createdAt.toDate(),
            VIVIENDA: doc.data().house_type === 0 ? "NUEVA" : "USADA",
            "NO. DE SOLICITUD": doc.data().number,
            ACREDITADO: doc.data().name,
            "TIPO DE CREDITO": doc.data().status,
            "FASE ACTUALIZADA": "",
            "OBSERVACION DE ACLARACION": "",
            CUV: "",
            "OBSERVACION DE EXPEDIENTE DE CREDITO": "",
            "FECHA DE FIRMA ACREDITADO": "",
            "ENTREGADO A MESA DE CONTROL": "",
            DESVIACIONES: "",
            "FECHA RECEPCION DE DESVIACION ": "",
          });
          idx++;
        });

        const fields = Object.keys(credits[0]);
        const opts = { fields };
        const parser = new Parser(opts);

        return parser.parse(credits);
      })
      .then((csv) => {
        // Step 4. Write the file to cloud function tmp storage
        return fs.outputFile(tempFilePath, csv);
      })
      .then(() => {
        // Step 5. Upload the file to Firebase cloud storage

        return storage.upload(tempFilePath, { destination: fileName });
      })
      .then((file) => {
        // Step 6. Sign Url from Upload
        return file[0].getSignedUrl({
          action: "read",
          expires: "03-09-2491",
        });
      })
      .then((url) => {
        return reportRef.update({ status: "completado", url: url });
      })
      .catch((err) => console.log(err));
  });

async function createDatasetIfNotExists(datasetName: any) {
  // Creates a client
  const bigqueryClient = new BigQuery();

  // Lists all datasets in the specified project
  const [datasets] = await bigqueryClient.getDatasets();
  const isExist = datasets.find((dataset: any) => dataset.id === datasetName);

  if (isExist) return Promise.resolve([isExist]);

  // Create the dataset
  return bigqueryClient.createDataset(datasetName);
}

async function executeQuery(query: any) {
  // Creates a client
  const bigqueryClient = new BigQuery({ projectId: "mifo-33ee4" });
  const sqlQuery = query;

  const options = {
    query: sqlQuery,
    timeoutMs: 100000, // Time out after 100 seconds.
    useLegacySql: false, // Use standard SQL syntax for queries.
  };

  // Runs the query
  return bigqueryClient.query(options);
}

// Cors response to handle the result from BigQuery
function responseCors(req: any, res: any, data: any) {
  return cors(req, res, () => {
    res.json({
      result: data,
    });
  });
}

// Cors error when the function throw an error
function errorCors(req: any, res: any, error: any) {
  return cors(req, res, () => {
    res.status(500).send(error);
  });
}

exports.bigQSqlQuery = functions.https.onRequest(async (req, res) => {
  try {
    //   Retrieve the request data from our Angular Application
    const data = req.body;
    const datasetName = data.datasetName;
    const query = data.query;

    if (!datasetName) {
      return errorCors(req, res, "Dataset name does not exists");
    }

    // If there is no query params from the App
    if (!query) {
      return errorCors(req, res, "Query does not exists");
    }

    await createDatasetIfNotExists(datasetName);

    // Execute the query and return a result Object if everything is OK
    const result = await executeQuery(query);
    return responseCors(req, res, result);
  } catch (error) {
    // Otherwise throw an error
    return errorCors(req, res, error);
  }
});

function createTable(dataSet: string, name: String) {
  const w = `
  CREATE TABLE IF NOT EXISTS ${dataSet}.${name} 
  (No STRING, UID STRING, Operator STRING, Creditor STRING, Tenmas_Operator STRING, cur_phase : INT64, completed: BOOLEAN, createAt TIMESTAMP, updatedAt TIMESTAMP)`;
  return executeQuery(w);
}

function insertCredit(dataSet: string, name: string, data: any) {
  const q = `
  INSERT INTO ${dataSet}.${name}
  (No, UID, Operator, Creditor, Tenmas_Operator, cur_phase, completed,  createAt, updatedAt)
  VALUES ('${data.number}', '${data.key}', ${data.owner}, ${data.name}, ${data.tenmas}, ${data.status}, ${data.completed}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())`;
  console.log(q);
  return executeQuery(q);
}

function updateCredit(dataSet: string, name: string, data: any) {
  const q = `
  UPDATE INTO ${dataSet}.${name}
  (Tenmas_Operator, cur_phase, completed, updatedAt)
  VALUES ('${data.tenmas}', '${data.status}', ${data.completed}, CURRENT_TIMESTAMP())`;
  console.log(q);
  return executeQuery(q);
}
