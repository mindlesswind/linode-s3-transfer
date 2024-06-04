import csv from 'csv-parser'
import fs from 'fs'
import path from 'path'
import { mkdir } from 'fs/promises'
import { Readable } from 'stream'
import 'dotenv/config'
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const csvFile = process.env.CSV_FILE_PATH 
const linodeBucketUrl = process.env.LINODE_BUCKET_URL

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

    fs.appendFileSync(path.resolve("./logs", `${date.toISOString()}.log`), `----Start Transfer----\n`)
    fs.appendFileSync(path.resolve("./logs", `${date.toISOString()}.log`), `Starting Time: ${date.toISOString()}\n`)

    fs.createReadStream(csvFile)
        .pipe(csv())
        .on('data', async (data) => {
            const res = await fetch(path.join(linodeBucketUrl, data.name));
            if (!fs.existsSync("tmp")) await mkdir("tmp");
            const destination = path.resolve("./tmp", data.name);
            const fileStream = fs.createWriteStream(destination, { flags: 'wx' });
            Readable.fromWeb(res.body).pipe(fileStream)
                .on('finish', () => {

                    const uploadCommand = new PutObjectCommand({
                        Bucket: process.env.S3_BUCKET_NAME,
                        Key: data.name,
                        Body: fs.createReadStream(destination),
                    })

                    s3Client.send(uploadCommand)
                        .then(() => {
                            fs.unlinkSync(destination)
                            fs.appendFileSync(path.resolve("./logs", `${date.toISOString()}.log`), `File ${data.name} downloaded successfully\n`)
                        })
                        .catch((err) => {
                            console.error(err)
                            fs.unlinkSync(destination)
                            fs.appendFileSync(path.resolve("./logs", `${date.toISOString()}.log`), `Error Uploading file ${data.name}\n`)
                        })
                })
                .on('error', (err) => {
                    if (err.code === 'EEXIST') {
                        console.log(`File already exists: ${err.path}`);
                        fs.appendFileSync(path.resolve("./logs", `${date.toISOString()}.log`), `File ${data.name} exists in parallel download, skip this time\n`)
                    } else {
                        console.error(err)
                        throw err
                    }

                })
        })
        .on('end', () => {
            console.log(`finish read list`)
            fs.appendFileSync(path.resolve("./logs", `${date.toISOString()}.log`), `Read Ending Time: ${new Date().toISOString()}\n`)
        });
} catch (err) {
    console.error(err)
}


