import * as functions from "firebase-functions";
import * as admin from "firebase-admin";

admin.initializeApp({
  credential: admin.credential.applicationDefault(),
  databaseURL: "https://mifo-33ee4.firebaseio.com/",
  storageBucket: "mifo-33ee4.appspot.com",
});

const { BigQuery } = require("@google-cloud/bigquery");
const { Parser } = require("json2csv");
const db = admin.firestore();
const fs_extra = require("fs-extra");
const fs = require("fs");
const path = require("path");
const os = require("os");
const csv = require("csv-parser");

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

// interface IUser {
//   name : string;
//   email: string;
// }

exports.userCreation = functions.https.onCall((data, context) => {
  const email = data.email;
  const domain = email.slice(email.indexOf("@") + 1, email.indexOf("."));
  const domainData = db
    .collection(`Domaines`)
    .where("domain", "==", domain)
    .get();

  const user_state = admin.auth().createUser({
    email: data.email,
    displayName: data.name,
    password: data.password,
    emailVerified: false,
  });
  let domainDataArr = true;
  return Promise.all([user_state, domainData])
    .then((user) => {
      domainDataArr = user[1].empty;
      return {
        email: user[0].email,
        name: user[0].displayName,
        user: user[0].uid,
        createdAt: admin.firestore.Timestamp.now(),
      };
    })
    .then((object) => {
      if (domainDataArr === true) {
        const domainAdd = db.collection("Domaines").add({
          domain: [domain],
          admin: [object.user],
        });

        const createDataset = createDatasetIfNotExists(domain);
        const createTablePromise = createTable(domain, "credits");

        const createAdmin = db.collection(`users`).add({
          domain: [domain],
          user_role: USER_ROLE.DEV_ADMIN,
          user_state: USER_STATUS.PENDING,
          ...object,
        });
        return Promise.all([domainAdd, createDataset, createTablePromise])
          .then(() => {
            return createAdmin;
          })
          .catch((e) => {
            console.log(e);
          });
      }

      if (domain === "tenmas") {
        return db.collection(`users`).add({
          domain: [domain],
          user_role: USER_ROLE.OPERATOR,
          user_state: USER_STATUS.PENDING,
          ...object,
        });
      }

      return db.collection("users").add({
        domain: [domain],
        user_role: USER_ROLE.DEV_OPERATOR,
        user_state: USER_STATUS.PENDING,
        ...object,
      });
    })
    .catch((e) => {
      console.error(e);
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
        phase: 0,
        confronta: {},
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

    const query_0 = insertCredit(data.domain[0], "credits", {
      key: id,
      operator: JSON.parse(data.dev_operator).key || "",
      operator_name: JSON.parse(data.dev_operator).name || "",
      phase: 0,
      completed: false,
      ...data,
    });

    const query_1 = insertCredit("tenmas", "credits", {
      key: id,
      operator: JSON.parse(data.tenmas_operator).key || "",
      operator_name: JSON.parse(data.tenmas_operator).name || "",
      phase: 0,
      completed: false,
      ...data,
    });

    const promises: any[] = [credit_update, notify, query_0, query_1];

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
  .onUpdate(async (snap, context) => {
    const id = context.params.id;
    const beforeDoc = snap.before.data();
    const afterDoc = snap.after.data();
    const tenmas_operator = JSON.parse(afterDoc.tenmas_operator).key || null;
    const dev_operator = JSON.parse(afterDoc.tenmas_operator).key || null;
    console.log(tenmas_operator);
    console.log(beforeDoc.tenmas_operator !== afterDoc.tenmas_operator);
    if (tenmas_operator === null) {
      return;
    }

    const promises: any[] = [];

    if (beforeDoc.tenmas_operator !== afterDoc.tenmas_operator) {
      //   const check_conv = await admin
      //     .database()
      //     .ref()
      //     .child(`conversations/${dev_operator}`)
      //     .once("child_added");
      //   console.log(check_conv.exists())
      //   if (check_conv.exists() === true) {
      //     const tenmas = admin
      //       .database()
      //       .ref()
      //       .child(`conversations/${dev_operator}`)
      //       .update({
      //         operatorName: JSON.parse(afterDoc.tenmas_operator).name || "",
      //       });
      //     const convData = await admin
      //       .database()
      //       .ref()
      //       .child(`conversations/${dev_operator}`)
      //       .once("child_changed");
      //     const newConv = admin
      //       .database()
      //       .ref()
      //       .child(`conversations/${tenmas_operator}/${convData.key}`)
      //       .set(convData.val());

      //     promises.push(tenmas);
      //     promises.push(newConv);
      //   }
      const query_1 = updateCredit("tenmas", afterDoc.number, {
        operator: JSON.parse(afterDoc.tenmas_operator).key || "",
        operator_name: JSON.parse(afterDoc.tenmas_operator).name || "",
        phase: afterDoc.phase || 0,
        completed: afterDoc.completed || false,
        ...afterDoc,
      });
      promises.push(query_1);
    }

    if (afterDoc.status === 3) {
      const notify = admin
        .firestore()
        .collection(`notifications`)
        .add({
          domain: ["tenmas", ...afterDoc.domain],
          owner: dev_operator.key,
          operator: tenmas_operator.key,
          viewed_by_owner: false,
          viewed_by_admin: false,
          viewed_by_operator: false,
          viewed_by_super_admin: false,
          priority: afterDoc.status,
          options: {
            credit_id: id,
          },
          createdAt: admin.firestore.Timestamp.now(),
        });
      promises.push(notify);
    }

    // Check that there was a change in the phase
    if (beforeDoc.phase < afterDoc.phase && beforeDoc.phase !== null) {
      const query_0 = updateCredit(afterDoc.domain[1], afterDoc.number, {
        operator: JSON.parse(afterDoc.dev_operator).key || "",
        operator_name: JSON.parse(afterDoc.dev_operator).name || "",
        phase: afterDoc.phase || 0,
        completed: afterDoc.completed || false,
        ...afterDoc,
      });

      const query_1 = updateCredit("tenmas", afterDoc.number, {
        operator: JSON.parse(afterDoc.tenmas_operator).key || "",
        operator_name: JSON.parse(afterDoc.tenmas_operator).name || "",
        phase: afterDoc.phase || 0,
        completed: afterDoc.completed || false,
        ...afterDoc,
      });

      promises.push(query_0);
      promises.push(query_1);
    }

    return Promise.all(promises)
      .then((message: any) => {
        console.log(message);
      })
      .catch((e: Error) => {
        console.error(e);
      });
  });

// Notify when Assigned to operator
function creditType(idx: number) {
  const type = ["Tradicional", "Conyugal", "Pensionado", "Mancomunado"];
  return type[idx];
}

function creditPhase(phase: string) {
  const creditSteps = [
    "GENERACION DE EXPEDIENTE",
    "ASIGNACION DE VIVIENDA",
    "SUPERVISION TECNICA",
    "VERIFICACION FINAL DE IMPORTES",
    "INSTRUCCION NOTARIAL (FECHA DE FIRMA)",
    "RESULTADO DE FIRMA DE ESCRITURAS",
    "SOLICITUD DE RECURSOS",
    "ASIGNACION DE RECURSOS",
    "PAGO A VENDEDOR",
    "FASES DE PAGO TERMINADAS",
    "EXPEDIENTE COMPLETO Y EN CUSTODIA",
  ];
  const idx = creditSteps.indexOf(phase);
  return idx > -1 ? idx : 0;
}
export const createCSV = functions.firestore
  .document("reports/{reportId}")
  .onCreate((snap, context) => {
    // Step 1. Set main variables

    const data = snap.data();

    const reportId = context.params.reportId;
    const fileName = `reports/${reportId}.csv`;
    const tempFilePath = path.join(os.tmpdir(), fileName);

    // Reference report in Firestore
    const reportRef = db.collection("reports").doc(reportId);

    // Reference Storage Bucket
    const storage = admin.storage().bucket(); // or set to env variable

    let query = db.collection("credits") as FirebaseFirestore.Query<
      FirebaseFirestore.DocumentData
    >;

    if (data.operator !== "") {
      if (data.domain[0] === "tenmas") {
        query = query.where("tenmas_operator", "==", data.operator);
      } else {
        query = query.where("dev_operator", "==", data.operator);
      }
    }

    if (data.fraccionamiento !== "") {
      query = query.where("fraccionamiento", "==", data.fraccionamiento);
    }

    if (data.start_date !== "") {
      query = query.where("createdAt", "<=", data.start_date);
    }

    query = query.where("domain", "array-contains", data.domain[0]);
    // Step 2. Query collection
    return query
      .get()
      .then((querySnapshot) => {
        /// Step 3. Creates CSV file from with orders collection
        const credits: any[] = [];

        // create array of users
        let idx = 0;
        if (querySnapshot.size === 0) {
          return Promise.reject("No Credits");
        }
        querySnapshot.forEach((doc) => {
          console.log(doc.data());
          console.log(doc.data().tenmas_operator);
          console.log(doc.data().dev_operator);
          const confronta = doc.data().confronta || {};
          const tenmas = JSON.parse(doc.data().tenmas_operator);
          const dev = JSON.parse(doc.data().dev_operator);

          credits.push({
            "No.": idx,
            EJECUTIVO: tenmas.name,
            PROMOTOR: doc.data().domain[1],
            ASESOR: dev.name,
            "RECEP. EXP. POR TEN MAS": doc.data().createdAt.toDate(),
            VIVIENDA: doc.data().house_type === 0 ? "NUEVA" : "USADA",
            "NO. DE SOLICITUD": doc.data().number,
            ACREDITADO: doc.data().name,
            "TIPO DE CREDITO": creditType(doc.data().credit_type),
            "FASE ACTUALIZADA": confronta.Phase || "",
            CUV: confronta.CUV || "",
            "FECHA DE FIRMA ACREDITADO": "",
            "ENTREGADO A MESA DE CONTROL": "",
            "Monto a Pagar": confronta.MontoFactura,
          });
          idx++;
        });

        const fields = Object.keys(credits[0]);
        const opts = { fields };
        const parser = new Parser(opts);

        return parser.parse(credits);
      })
      .then((_csv) => {
        // Step 4. Write the file to cloud function tmp storage
        return fs_extra.outputFile(tempFilePath, _csv);
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
      .catch(async (err) => {
        await reportRef.update({ status: "error" }).catch((e) => {
          console.log(e);
        });
        console.log(err);
      });
  });

exports.generateThumbnail = functions.storage
  .object()
  .onFinalize(async (object) => {
    const fileBucket = object.bucket; // The Storage bucket that contains the file.
    const filePath = object.name as string; // File path in the bucket.

    if (!filePath.startsWith("Confronta/")) {
      return;
    }

    const fileName = path.basename(filePath);

    const bucket = admin.storage().bucket(fileBucket);
    const tempFilePath = path.join(os.tmpdir(), fileName);

    await bucket.file(filePath).download({ destination: tempFilePath });

    const creditData = await db.collection("credits").get();

    const credits: any[] = [];
    const promises: Promise<FirebaseFirestore.WriteResult>[] = [];

    fs.createReadStream(tempFilePath)
      .pipe(csv())
      .on("data", (row: any) => {
        console.log(row["No Solicitud"]);
        console.log(row["Monto a Ejercer Titular"]);
        const confronta = {
          key: row["No Solicitud"],
          CUV: row["CUV"],
          MontoFactura: row["Monto a Ejercer Titular"],
          Phase: row["Fase"],
          PhaseNo: creditPhase(row["Fase"]),
        };
        credits.push(confronta);
      })
      .on("end", () => {
        console.log("CSV file successfully processed");

        credits.forEach((credit) => {
          console.log(credit.key);
          const idx = creditData.docs.findIndex(
            (x) => `'${x.data().number}` === credit.key
          );
          if (idx > -1) {
            const id = creditData.docs[idx].id;
            console.log(credit);
            const update = db
              .collection("credits")
              .doc(id)
              .update({ phase: credit.PhaseNo, confronta: credit });
            promises.push(update);
          }
        });
        return Promise.all(promises);
      });
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
  (CREDIT_ID STRING, UID STRING, Operator STRING, Operator_name STRING, Creditor STRING, cur_phase INT64, completed BOOLEAN, createAt TIMESTAMP, updatedAt TIMESTAMP)`;
  return executeQuery(w);
}

function insertCredit(dataSet: string, name: string, data: any) {
  console.log(
    `'${data.number}', '${data.key}', '${data.operator}', '${data.name}', '${data.operator_name}', ${data.phase}, ${data.completed}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()`
  );
  const q = `
  INSERT INTO ${dataSet}.${name}
          (CREDIT_ID, UID, Operator, Operator_name, Creditor, cur_phase, completed,  createAt, updatedAt)
  VALUES  ('${data.number}', '${data.key}', '${data.operator}', '${data.operator_name}', '${data.name}', ${data.phase}, ${data.completed}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())`;
  console.log(q);
  return executeQuery(q);
}

function updateCredit(dataSet: string, credit: string, data: any) {
  if (data.operator == "") {
    const q_0 = `
    UPDATE ${dataSet}.credits
    SET cur_phase = ${data.phase}, updatedAt = CURRENT_TIMESTAMP()
    Where CREDIT_ID = '${credit}'`;
    console.log(q_0);
    return executeQuery(q_0);
  }
  const q_1 = `
  UPDATE ${dataSet}.credits
  SET cur_phase = ${data.phase}, updatedAt = CURRENT_TIMESTAMP(), Operator = '${data.operator}', Operator_name = '${data.operator_name}'
  Where CREDIT_ID = '${credit}'`;
  console.log(q_1);
  return executeQuery(q_1);
}

exports.activate = functions.https.onRequest((req, res) => {
  admin
    .auth()
    .updateUser("vZgZKMy6pDW4yxGhnMYMAo1nOdB2", {
      emailVerified: true,
    })
    .then(() => {
      console.log("Finished");
      return res.status(200);
    })
    .catch((e) => {
      console.log(e);
      return res.status(400);
    });
});
