import os
from graphrag.index.utils import gen_md5_hash,gen_sha256_hash
from graphrag.common.storage.queue_storage import QueueStorageClient
from graphrag.common.storage.blob_pipeline_storage import BlobPipelineStorage
import json
import base64
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient
from azure.storage.queue import QueueServiceClient, QueueClient, QueueMessage, BinaryBase64DecodePolicy, BinaryBase64EncodePolicy
import asyncio

def find_next_target_index_blob(queue_storage_client: QueueStorageClient, watermark_client: BlobPipelineStorage, caller: str):
    # queue_url="https://inputdatasetsa.queue.core.windows.net",
    # queue_name="inputdataetqu"
    # client_id = "500051c4-c242-4018-9ae4-fb983cfebefd"
    # queue_url = os.environ.get("AZURE_QUEUE_URL")
    # queue_name = os.environ.get("AZURE_QUEUE_NAME")
    # client_id = os.environ.get("AZURE_CLIENT_ID")
    #max_messages = os.environ.get("MAX_QUEUE_MESSAGE_COUNT", default=1)

    messages = queue_storage_client.poll_messages()
    to_process_msgs = []
    for message in messages:
        content = str(base64.b64decode(message.content).decode('utf-8'))
        target_blob = BlobClient.from_blob_url(json.loads(content)['data']['url']).blob_name
        
        if check_if_watermark_exitst(target_blob=target_blob, watermark_client=watermark_client, caller=caller) is True:
            queue_storage_client.delete_message(message)
        else:
            target_blob = parse_message_get_blob(message=message, caller=caller)
            target_blob.append(message)
            to_process_msgs.append(target_blob)
    
    
    return to_process_msgs

def parse_message_get_blob(message: QueueMessage, caller: str) -> str:
    if caller == 'indexer':
        content = str(base64.b64decode(message.content).decode('utf-8'))
        target_blob_url = json.loads(content)['data']['url']
        blob_client = BlobClient.from_blob_url(target_blob_url)

        return [blob_client.blob_name, blob_client.blob_name]
    
    if caller == 'context':
        content = str(base64.b64decode(message.content).decode('utf-8'))
        target_blob_url = json.loads(content)['data']['url']
        blob_client = BlobClient.from_blob_url(target_blob_url)

        blob_name = blob_client.blob_name.split("/")[-1]

        return [blob_client.blob_name, f"{blob_name}/version=0"]



def check_if_watermark_exitst(target_blob: str, watermark_client: BlobPipelineStorage, caller: str) -> bool:
    if caller == 'indexer':
        keys = { "key" : target_blob }
        target_guid = gen_md5_hash(keys, keys.keys())
        target_watermark_blob = f"{target_guid}.watermark"
        result : bool = watermark_client.check_if_exists(target_watermark_blob)
        return result

    if caller == 'context':
        if target_blob.endswith('_init.json'):
            return True
        else:
            return False
    
    return True

def water_mark_target(targets: list, queue_storage_client: QueueStorageClient, watermark_client: BlobPipelineStorage, path_prefix: str = None):
    for target in targets:
        key = target[0]
        message : QueueMessage = target[1]

        keys = { "key" : key }
        target_guid = gen_md5_hash(keys, keys.keys())
        target_watermark_blob = f"{target_guid}.watermark"
        if path_prefix is not None and len(path_prefix) > 0:
            target_watermark_blob = f"{path_prefix}/{target_guid}.watermark"
        asyncio.run(watermark_client.set(key=target_watermark_blob, value=""))

        queue_storage_client.delete_message(message=message)