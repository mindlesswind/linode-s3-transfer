import csv from 'csv-parser'
import fs from 'fs'
import path from 'path'
import { mkdir } from 'fs/promises'
import { Readable } from 'stream'
import 'dotenv/config'
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { fetch, Agent } from 'undici';
import mime from 'mime';


const csvFile = process.env.CSV_FILE_PATH
const linodeBucketUrl = process.env.LINODE_BUCKET_URL

let fileRecords = []

const date = new Date();

const s3Client = new S3Client(
    {
        region: process.env.S3_AWS_REGION,
        credentials: {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID,
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
        }
    }
);

try {
    if (!fs.existsSync("logs")) await mkdir("logs");

    logging(`${"=".repeat(20)}Start Transfer${"=".repeat(20)}`)
    logging(`Starting Time: ${date.toISOString()}\n`)

    fs.createReadStream(csvFile)
        .pipe(csv())
        .on('data', (data) => {
            try {
                if (data['format'].toLowerCase() === 's3'){
                    fileRecords.push([data.id, data.name])    
                }
            } catch{
                logging(`Skip ${data.id} ${data.name}, due to missing field`)
            }
        })
        .on('end', async () => {

            logging(`Read Ending Time: ${new Date().toISOString()}`)

            logging(`Start Transfer Time: ${new Date().toISOString()}`)
            logging(`${"=".repeat(50)}\n`)

            while (fileRecords.length > 0) {

                let processList = []

                for (let i = 0; i < (process.env.UPLOAD_THREADS ?? 10); i++) {
                    if (fileRecords.length > 0) {
                        const record = fileRecords.shift()
                        processList.push(transferFile(record[0], record[1], date))
                    } else {
                        break
                    }
                }

                const results = await Promise.all(processList)

                results.forEach(result => {
                    if (result.success) {
                        logging(`ID ${result.id} | File ${result.key} OK`)
                    } else {
                        logging(`ID ${result.id} | File ${result.key} | ${result.reason}`)
                    }
                })
            }

            logging(`\n${"=".repeat(50)}\n`)
            logging(`End Transfer Time: ${new Date().toISOString()}`)
        });
} catch (err) {
    console.error(err)
}

async function transferFile(id, key) {
    try {
        const res = await fetch(path.join(linodeBucketUrl, key), {
            dispatcher: new Agent({
                bodyTimeout: 10 * 60e3, // 10 minutes
            })
        })

        if (res.status !== 200) {
            return { success: false , key: key, id: id, reason: `Failed to download from Linode Bucket`};
        }

        if (!fs.existsSync("tmp")) await mkdir("tmp");

        const destination = path.resolve("./tmp", key);

        try {
            const fileStream = fs.createWriteStream(destination, { flags: 'wx' });

            const pipe = Readable.fromWeb(res.body).pipe(fileStream)

            await new Promise((resolve, reject) => { pipe.on("finish", resolve); pipe.on("error", reject) });

        } catch (err) {
            if (err.code === 'EEXIST') {
                return { success: false , key: key, id: id, reason: `Parallel Download Error: ${err.message}`};
            } else {
                return { success: false , key: key, id: id, reason: `Write File Error: ${err.message}`};
            }
        }

        const uploadCommand = new PutObjectCommand({
            Bucket: process.env.S3_BUCKET_NAME,
            Key: key,
            Body: fs.createReadStream(destination),
            ContentType: mime.getType(destination)
        })

        try {
            const uploadResult = await s3Client.send(uploadCommand)
            fs.unlinkSync(destination)
        } catch (err) {
            fs.unlinkSync(destination)
            return { success: false , key: key, id: id, reason: `Upload Error: ${err.message}`};
        }

        return { success: true , key: key, id: id};

    } catch (err) {
        return { success: false , key: key, id: id, reason: `Error: ${err.message}`};
    }
}

function logging(string) {
    console.log(`${string}`);
    fs.appendFileSync(path.resolve("./logs", `${date.toISOString()}.log`), `${string}\n`)
}
