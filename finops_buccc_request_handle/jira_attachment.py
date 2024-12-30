import requests
import json
import re

class attachment:
    def __init__(self, data, auth):
        self.data = data
        self.auth = auth

    def __repr__(self) -> str:
        return self.data['filename']
        
    def get_size(self):
        return self.data["size"]

    def get_name(self):
        return re.sub(" \(\S{8}-\S{4}-\S{4}-\S{4}-\S{12}\)", "", self.data["filename"])
    def get_decrypted_content(self,kms_client, key_arn):
        encrypted_content = self.get_content()
        content = kms_client.decrypt(
            CiphertextBlob=encrypted_content,
            KeyId=key_arn,
        )['Plaintext']
        return content
    def get_content(self) -> bytes:
        request = requests.get(self.data["content"], auth=self.auth)
        if request.ok:
                return request.content
        else:
            raise Exception(
                f"{request.text}\nError in jira.client.issue.attachment.get_content"
            )
    def get_media_id(self):
        request = requests.get(self.data['content'], auth=self.auth,allow_redirects=False)
        media_id = request.headers['Location'].split('/')[4]
        return media_id
    
    def delete(self):
        request = requests.delete(url=f"{self.data['self']}",auth=self.auth)
        if request.ok:
            self.data = {}
            return True
        else:
            raise Exception("Error in jira.client.issue.attachment.delete")