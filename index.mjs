import csv from 'csv-parser'
import fs from 'fs'
import path from 'path'
import { mkdir } from 'fs/promises'
import { Readable } from 'stream'
import 'dotenv/config'
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { fetch, Agent } from 'undici';
import { log } from 'console'

const csvFile = process.env.CSV_FILE_PATH
const linodeBucketUrl = process.env.LINODE_BUCKET_URL

let fileNames = []

try {
    const s3Client = new S3Client(
        {
            region: process.env.S3_AWS_REGION,
            credentials: {
                accessKeyId: process.env.AWS_ACCESS_KEY_ID,
                secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
            }
        }
    );

    const date = new Date();
    if (!fs.existsSync("logs")) await mkdir("logs");

    logging(date, `${"=".repeat(20)}Start Transfer${"=".repeat(20)}`)
    logging(date, `Starting Time: ${date.toISOString()}\n`)

    fs.createReadStream(csvFile)
        .pipe(csv())
        .on('data', (data) => {
            fileNames.push(data.name)
        })
        .on('end', async () => {

            logging(date, `Read Ending Time: ${new Date().toISOString()}`)

            const uniqueFileNamesSet = [...new Set(fileNames)]
            const uniqueFileNames = Array.from(uniqueFileNamesSet)

            logging(date, `Start Transfer Time: ${new Date().toISOString()}`)
            logging(date, `${"=".repeat(50)}\n`)

            for (let key of uniqueFileNames) {
                const res = await fetch(path.join(linodeBucketUrl, key), {
                    dispatcher: new Agent({
                        bodyTimeout: 10 * 60e3, // 10 minutes
                    })
                })

                if (!fs.existsSync("tmp")) await mkdir("tmp");
                const destination = path.resolve("./tmp", key);
                try {
                    const fileStream = fs.createWriteStream(destination, { flags: 'wx' });

                    const pipe = Readable.fromWeb(res.body).pipe(fileStream)
                    const downloadResult = await new Promise((resolve, reject) => { pipe.on("finish", resolve); pipe.on("error", reject) });
                } catch (err) {
                    if (err.code === 'EEXIST') {
                        logging(date, `File ${data.name} exists in parallel download, skip this time`)
                    } else {
                        console.error(err)
                        throw err
                    }
                }

                const uploadCommand = new PutObjectCommand({
                    Bucket: process.env.S3_BUCKET_NAME,
                    Key: key,
                    Body: fs.createReadStream(destination),
                })
                try {
                    const uploadResult = await s3Client.send(uploadCommand)
                    logging(date, `File ${key} uploaded successfully`)
                    fs.unlinkSync(destination)
                } catch (err) {
                    fs.unlinkSync(destination)
                    logging(date, `File ${key} failed to upload`)
                }
            }

            logging(date, `\n${"=".repeat(50)}\n`)
            logging(date, `End Transfer Time: ${new Date().toISOString()}`)
        });
} catch (err) {
    console.error(err)
}



function logging(date, string) {
    console.log(`${string}`);
    fs.appendFileSync(path.resolve("./logs", `${date.toISOString()}.log`), `${string}\n`)
}
