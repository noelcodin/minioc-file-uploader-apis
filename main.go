package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	//Pass params bucket & folder, return all the file names under them
	case "GET":
		params := r.URL.Query()
		bucket, b_ok := params["bucket"]
		folder, f_ok := params["folder"]
		//Parse params, return error if either is missing
		if !b_ok || !f_ok {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"message": "params error, both bucket and folder are required"}`))
		} else {
			// retrieve all the objects under the selected bucket and folder
			err, objs := ListBucketObjs(bucket[0], folder[0])
			if err || objs == nil {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"message": "Data not found"}`))
			} else {
				w.WriteHeader(http.StatusOK)
				//In response, join multiple file names as one string
				res_map := map[string]string{"message": "get called, bucket: " + bucket[0] + ", folder: " + folder[0] + ", files: " + strings.Join(objs, ",") + ", file count: " + strconv.Itoa(len(objs))}
				res, _ := json.Marshal(res_map)
				w.Write(res)
			}

		}
		//Pass params bucket & folder, filter all the files under them, include all the json content into 1 json file
	case "POST":
		params := r.URL.Query()
		bucket, b_ok := params["bucket"]
		folder, f_ok := params["folder"]
		//Parse params, return error if either is missing
		if !b_ok || !f_ok {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"message": "params error, both bucket and folder are required"}`))
		} else {
			err, files := ListBucketObjs(bucket[0], folder[0])
			if err || files == nil {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"message": "Data not found"}`))
			}
			res := make(map[string]map[string]interface{})
			file_c := 0
			for _, file_name := range files {
				if !strings.Contains(file_name, ".json") {
					continue
				}
				file_c += 1
				ch := make(chan map[string]interface{})
				// use coroutine to multi-process the download file functions
				go coroutine_for_batch_file_download(file_name, bucket[0], folder[0], ch)
				// retrieve json content from channel signal
				res[file_name] = <-ch
			}
			// lock until all file content has been collected
			for len(res) < file_c {
				time.Sleep(1000)
			}
			w.WriteHeader(http.StatusOK)
			res1, _ := json.Marshal(res)
			w.Write(res1)

		}
		//Pass params bucket & folder & file_name, store the file content to minio server
	case "PUT":
		params := r.URL.Query()
		bucket, b_ok := params["bucket"]
		folder, f_ok := params["folder"]
		file_name, f_ok1 := params["file_name"]
		if !strings.Contains(file_name[0], ".json") {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"message": "params error, only accept uploading .json files"}`))

		}
		//Parse params, return error if any is missing
		body, err := ioutil.ReadAll(r.Body)
		// if fail to read the response body
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			w.Write([]byte(`{"message": "Server error"}`))
		}
		// if param is missing or no valid file input
		if !b_ok || !f_ok || body == nil || !f_ok1 {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"message": "params error, both bucket, folder, file_name and file bytes in body are required"}`))
		} else {
			// check if the dest bucket exists
			found := BucketExists(bucket[0])
			// create the bucket if it does not exist
			if !found {
				CreateBucket(bucket[0])
			} else {
				// write input bytes into local temp file
				output_path := "tmp/upload/" + bucket[0] + "/" + folder[0] + "/" + file_name[0]
				err2 := os.MkdirAll("tmp/upload/"+bucket[0]+"/"+folder[0], 0766)
				stringfrombytes := string(body)
				//clean up the json strings
				for _, substring := range []string{"--", "Content-Disposition:"} {
					re := regexp.MustCompile(substring + ".*")
					stringfrombytes = re.ReplaceAllString(stringfrombytes, "")
				}
				destFile, err := os.Create(output_path)
				destFile.WriteString(strings.TrimSpace(stringfrombytes))
				file, err1 := os.Open(output_path)
				// pass the temp file and other params to minio and upload the file
				err3, success := PutObject(bucket[0], folder[0], file, file_name[0])
				log.Println(err3)
				// if any error happens during file process or upload request fails
				if err != nil || err2 != nil || err1 != nil || !success {
					w.WriteHeader(http.StatusBadGateway)
					w.Write([]byte(`{"message": "Server error"}`))

				} else {
					// if successfully uploads the file
					w.WriteHeader(http.StatusOK)
					res_map := map[string]string{"message": "put called, " + file_name[0] + " is saved in bucket: " + bucket[0] + ", folder: " + folder[0]}
					res, _ := json.Marshal(res_map)
					w.Write(res)
				}

			}
		}
		// no effect
	case "DELETE":
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message": "delete called"}`))
	default:
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"message": "not found"}`))
	}
}

func coroutine_for_batch_file_download(file_name string, bucket string, folder string, ch chan map[string]interface{}) {
	_, obj := DownloadObject(bucket, folder, file_name)
	// creates a bytes.Buffer and read from io.Reader
	buf := &bytes.Buffer{}
	buf.ReadFrom(obj)

	// retrieve a byte slice from bytes.Buffer
	bytes := buf.Bytes()
	stringfrombytes := string(bytes)
	for _, substring := range []string{"--", "Content-Disposition:"} {
		// clean up the json strings
		re := regexp.MustCompile(substring + ".*")
		stringfrombytes = re.ReplaceAllString(stringfrombytes, "")
	}
	var jsonMap map[string]interface{}
	json.Unmarshal([]byte(strings.TrimSpace(stringfrombytes)), &jsonMap)
	ch <- jsonMap
}

//Establish connection with a minio server
//Replcae endpoint, accessKeyID, secretAccessKey with your own server settings
func Conn() (error, *minio.Client) {
	endpoint := "127.0.0.1:9000"
	accessKeyID := "noel"
	secretAccessKey := "123456EXAMPLEKEY"
	useSSL := false

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}

	//log.Printf("%#v\n", minioClient) // minioClient is now setup
	return err, minioClient
}

//Check if a bucket exists
func BucketExists(bucket string) bool {
	err, minioClient := Conn()
	if err != nil {
		log.Fatalln(err)
		return false
	}
	found, err := minioClient.BucketExists(context.Background(), bucket)
	if err != nil {
		log.Fatalln(err)
		return false
	}
	if found {
		//log.Println("Bucket found")
		return true
	} else {
		log.Println("Bucket not found")
		return false
	}
}

//Create a bucket
func CreateBucket(bucket string) bool {
	err, minioClient := Conn()
	if err != nil {
		log.Fatalln(err)
		return false
	}
	//If the bucket alr exists, skip requesting the monio server
	found := BucketExists(bucket)
	if found {
		//log.Println("Bucket found")
		return true
	} else {
		// If the bucket not exists, create a bucket at region 'us-east-1' with object locking enabled.
		err = minioClient.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: "us-east-1"})
		if err != nil {
			log.Fatalln(err)
			return false
		}
		log.Println("Successfully created the bucket.")
		return true
	}
}

//Filter objects by bucket and folder, return an array of file names
func ListBucketObjs(bucket string, folder string) (bool, []string) {
	err, minioClient := Conn()
	if err != nil {
		log.Fatalln(err)
		return true, nil
	}
	found := BucketExists(bucket)
	if !found {
		log.Println("Bucket not found")
		return true, nil
	} else {
		ctx, cancel := context.WithCancel(context.Background())

		defer cancel()

		objectCh := minioClient.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			Prefix:    folder + "/",
			Recursive: true,
		})
		// Parse the eligible file name and append to the res array
		var res []string
		for object := range objectCh {
			if object.Err != nil {
				log.Fatalln(object.Err)
				return true, nil
			}
			res = append(res, strings.Split(object.Key, "/")[1])
		}
		return false, res
	}
}

// Upload a file
func PutObject(bucket string, folder string, file *os.File, file_name string) (bool, bool) {
	err, minioClient := Conn()
	if err != nil {
		log.Fatalln(err)
		return true, false
	}
	found := BucketExists(bucket)
	// Return error if the bucket does not exist
	if !found {
		log.Println("Bucket not found")
		return true, false
	} else {
		defer file.Close()
		// password for object encrypting
		//password := "some key"
		fileStat, err := file.Stat()
		if err != nil {
			log.Println(err)
			return true, false
		}
		// initiate the minio encryption
		//encryption := encrypt.DefaultPBKDF([]byte(password), []byte(bucket+file_name))
		//uploadInfo, err := minioClient.PutObject(context.Background(), bucket, folder+"/"+file_name, file, fileStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream", ServerSideEncryption: encryption})
		uploadInfo, err := minioClient.PutObject(context.Background(), bucket, folder+"/"+file_name, file, fileStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err != nil {
			log.Println(err)
			return true, false
		}
		log.Println("Successfully uploaded bytes: ", uploadInfo)
		return false, true
	}
}

//Download a file
//Specify the bucket, folder, file name
func DownloadObject(bucket string, folder string, file_name string) (bool, *minio.Object) {
	err, minioClient := Conn()
	if err != nil {
		log.Fatalln(err)
		return true, nil
	}
	found := BucketExists(bucket)
	if !found {
		log.Println("Bucket not found")
		return true, nil
	} else {
		/***err1, exist := ObjectExists(bucket, folder, file_name)
		if err1 || !exist {
			return false, nil
		}***/
		// password for object decrypting
		//password := "some key"
		// initiate the minio encryption
		//encryption := encrypt.DefaultPBKDF([]byte(password), []byte(bucket+file_name))
		//object, err := minioClient.GetObject(context.Background(), bucket, folder+"/"+file_name, minio.GetObjectOptions{ServerSideEncryption: encryption})
		object, err := minioClient.GetObject(context.Background(), bucket, folder+"/"+file_name, minio.GetObjectOptions{})
		if err != nil {
			log.Println(err)
			return true, nil
		}
		return false, object
	}
}

//Check whether a file exists
//Specify the bucket, folder, file name
func ObjectExists(bucket string, folder string, file_name string) (bool, bool) {
	err, minioClient := Conn()
	if err != nil {
		log.Println(err)
		return true, false
	}
	found := BucketExists(bucket)
	if !found {
		log.Println("Bucket not found")
		return true, false
	} else {
		err = minioClient.FGetObject(context.Background(), bucket, file_name, folder+"/"+file_name, minio.GetObjectOptions{})
		if err != nil {
			log.Println(err)
			return false, true
		}
	}
	return false, false
}

// entry to lauch the http server
//due to the dependency functions locate in another file, shld lauch the whole project by "go run ${all the .go files}" instead of only this file
func main() {
	http.HandleFunc("/", ServeHTTP)
	//launch http endpoint
	//err := http.ListenAndServe(":8080", nil)

	//lauch https endpoints
	certFile := "server.crt"
	keyFile := "server.key"
	err := http.ListenAndServeTLS(":8080", certFile,
		keyFile, nil)

	if err != nil {
		log.Fatal(err.Error())
	}
}
