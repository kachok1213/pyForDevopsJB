import boto3
import uuid


#create uuid format name
def create_bucket_name(bucket_prefix):
    try:
    # The generated bucket name must be between 3 and 63 chars long
        return ''.join([bucket_prefix, str(uuid.uuid4())])
    except Exception as e:
        print("Exception while creating a bucket name ------>"+str(e))

#create bucket
def create_bucket(bucket_prefix, s3_connection,region):
    try:
        session = boto3.session.Session(region_name=region)
        print('---------------------------------')
        print(session)
        print('---------------------------------')
        current_region = region
        print('---------------------------------')
        print(current_region)
        print('---------------------------------')
        bucket_name = bucket_prefix
        print("bucket_name")
        print(bucket_name)
        bucket_response = s3_connection.create_bucket(
        Bucket=bucket_name)
        print(bucket_name, current_region)
        return bucket_name, bucket_response
    except Exception as e:
        print("Exception while creating a bucket ------>"+str(e))

#create txt file
def create_temp_file(size, file_name, file_content):
    try:
        random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
        with open(random_file_name, 'w') as f:
            f.write(str(file_content) * size)
        return random_file_name
    except Exception as e:
        print("Exception while creating a file ------>"+str(e))

def copy_to_bucket(bucket_from_name, bucket_to_name, file_name):
    try:
        copy_source = {
            'Bucket': bucket_from_name,
            'Key': file_name
        }
        s3_resource.Object(bucket_to_name, file_name).copy(copy_source)
    except Exception as e:
        print("Exception while copying from bucket "+bucket_from_name+"to"+bucket_to_name+" ------>"+str(e))

def enable_bucket_versioning(bucket_name):
    try:
        bkt_versioning = s3_resource.BucketVersioning(bucket_name)
        bkt_versioning.enable()
        print(bkt_versioning.status)
    except Exception as e:
        print("Exception while enableing bucket versioning ------>"+str(e))

def delete_all_objects(bucket_name):
    res = []
    try:
        bucket=s3_resource.Bucket(bucket_name)
        for obj_version in bucket.object_versions.all():
            res.append({'Key': obj_version.object_key,
                    'VersionId': obj_version.id})
        print(res)
        bucket.delete_objects(Delete={'Objects': res})    
    except Exception as e:
        print("Exception while deleting all objects ------>"+str(e))
    
    

if __name__ == '__main__':
        
#variables definition
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    current_regionparam = 'us-east-1'

#create file
    first_file_name = create_temp_file(300, 'firstfile.txt', 'f')   
    second_file_name = create_temp_file(400, 'secondfile.txt', 's')
    third_file_name = create_temp_file(300, 'thirdfile.txt', 't')
#first bucket name
    first_bucket_name = create_bucket_name("name")#first_bucket.name
    second_bucket_name = create_bucket_name("name")#second_bucket.name
    print("Client-Resource")
    print(s3_client)
    print(s3_resource)
    create_bucket(first_bucket_name,s3_client,current_regionparam)
    create_bucket(second_bucket_name,s3_resource,current_regionparam)
#bucket obj/instance
    first_bucket = s3_resource.Bucket(name=first_bucket_name)
    first_object = s3_resource.Object(bucket_name=first_bucket_name, key=first_file_name)
    second_bucket = s3_resource.Bucket(name=second_bucket_name)
    second_object = s3_resource.Object(first_bucket_name, second_file_name)
    print("Step1 - Done")
    print(first_bucket)
    print(first_object)
    print(second_bucket)
    print(second_object)

#upload files
    first_object.upload_file(first_file_name)
    second_object.upload_file(second_file_name, ExtraArgs={'ACL': 'public-read'})
    second_object_acl = second_object.Acl()
    response = second_object_acl.put(ACL='public-read')
    third_object = s3_resource.Object(first_bucket_name, third_file_name)
    third_object.upload_file(third_file_name, ExtraArgs={'ServerSideEncryption': 'AES256'})
    third_object.upload_file(third_file_name, ExtraArgs={'ServerSideEncryption': 'AES256','StorageClass': 'STANDARD_IA'})

    #first_bucket.upload_file(Filename=first_file_name, Key=first_file_name)
    #s3_client.upload_file(Filename=first_file_name, Bucket=first_bucket_name,Key=first_file_name)

#download files
    s3_resource.Object(first_bucket_name, first_file_name).download_file(f'tmp/{first_file_name}') 

#copy obj between buckets
    copy_to_bucket(first_bucket_name, second_bucket_name, first_file_name)

    s3_resource.Object(second_bucket_name, first_file_name).delete()


    third_object.reload()
    third_object.storage_class

    print(third_object)
    print(third_file_name)
    third_object.upload_file(third_file_name, ExtraArgs={
                         'ServerSideEncryption': 'AES256', 
                         'StorageClass': 'STANDARD_IA'})


    third_object.reload()
    third_object.storage_class


    enable_bucket_versioning(first_bucket_name)

    s3_resource.Object(first_bucket_name, first_file_name).upload_file(
    first_file_name)
    s3_resource.Object(first_bucket_name, first_file_name).upload_file(
    third_file_name)


    s3_resource.Object(first_bucket_name, second_file_name).upload_file(
    second_file_name)

    s3_resource.Object(first_bucket_name, first_file_name).version_id
    'eQgH6IC1VGcn7eXZ_.ayqm6NdjjhOADv'

    for bucket in s3_resource.buckets.all():
        print(bucket.name)


    for bucket_dict in s3_resource.meta.client.list_buckets().get('Buckets'):
        print(bucket_dict['Name'])

    for obj in first_bucket.objects.all():
        print(obj.key)


# for obj in first_bucket.objects.all():
#        subsrc = obj.Object()
#        print(obj.key, obj.storage_class, obj.last_modified,
#              subsrc.version_id, subsrc.metadata)

#delete_all_objects(first_bucket_name)
#s3_resource.Bucket(first_bucket_name).delete()
#s3_resource.meta.client.delete_bucket(Bucket=second_bucket_name)
