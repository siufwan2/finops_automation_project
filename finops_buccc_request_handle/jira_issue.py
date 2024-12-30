import requests
import json
import re
from jira_attachment import attachment
class issue:
    def __init__(self, data, auth, fields_metadata):
        self.data = data
        self.auth = auth
        self.fields_metadata = fields_metadata
        

    def get_forms_url(self):
        baseurl = '/'.join(self.data['self'].split('/')[:3])
        response = requests.get(
            f"{baseurl}/_edge/tenant_info",
            auth=self.auth,
        )
        if response.ok:
            return f'https://api.atlassian.com/jira/forms/cloud/{response.json()["cloudId"]}'
        else:
            raise Exception(f"Error in jira.client.get_cloud_id\n{response.text}")
        
    def get_reporter(self):
        return self.data['fields']['reporter']
    def get_linked_issues(self,link_type):
        def get_issues(issue_links):
            result = []
            for issue_link in issue_links:
                request = requests.get(issue_link,auth=self.auth)
                if request.ok:
                    result.append(issue(
                    data=request.json(), auth=self.auth, fields_metadata=self.fields_metadata
                ))
                else:
                    raise Exception(f"{request.text}\nError in jira.client.issue.get_linked_issues")
            return result
        all_linked_issues = list(filter(lambda x: x['type']['name'] == link_type , self.get_links()))
        inward_issues = [ x['inwardIssue']['self'] for x in all_linked_issues if x.get('inwardIssue')]
        outward_issues = [ x['outwardIssue']['self'] for x in all_linked_issues if x.get('outwardIssue')]
        return {
            'inwards': get_issues(inward_issues),
            'outwards': get_issues(outward_issues)
        }

    def link_related_issues(self,issues):
        key_list = filter(lambda x: True if x != self.get_key() else False,map(lambda x:x.get_key(), issues))
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        baseurl = f'https://{self.data["self"].split("/")[2]}'
        for key in key_list:
            payload = json.dumps( {
                "inwardIssue": {
                    "key": self.get_key()
                },
                "outwardIssue": {
                    "key": key
                },
                "type": {
                    "name": 'Relates'
                }
                } )
            request = requests.post(f'{baseurl}/rest/api/3/issueLink',data=payload,headers=headers,auth=self.auth)
    def link_approved_child_issues(self,issues):
        key_list = filter(lambda x: True if x != self.get_key() else False,map(lambda x:x.get_key(), issues))
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        baseurl = f'https://{self.data["self"].split("/")[2]}'
        for key in key_list:
            payload = json.dumps( {
                "inwardIssue": {
                    "key": self.get_key()
                },
                "outwardIssue": {
                    "key": key
                },
                "type": {
                    "name": 'Approval'
                }
                } )
            request = requests.post(f'{baseurl}/rest/api/3/issueLink',data=payload,headers=headers,auth=self.auth)
    def get_links(self):
        return self.data['fields']['issuelinks']
    def is_approved(self):
        status = 'approved'
        parent_issues = self.get_linked_issues('Approval')['inwards']
        if len(parent_issues) == 0:
            return 'NA'
        for parent in parent_issues:
            if parent.get_resolution():
                return 'rejected'
            elif parent.get_field('Approval_For_Child_Ticket/DA') != 'approved':
                status = 'not approved'
        return status

    def link_blocked_issues(self,issues):
        key_list = filter(lambda x: True if x != self.get_key() else False,map(lambda x:x.get_key(), issues))
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        baseurl = f'https://{self.data["self"].split("/")[2]}'
        for key in key_list:
            payload = json.dumps( {
                "inwardIssue": {
                    "key": key
                },
                "outwardIssue": {
                    "key": self.get_key()
                },
                "type": {
                    "name": 'Blocks'
                }
                } )
            request = requests.post(f'{baseurl}/rest/api/3/issueLink',data=payload,headers=headers,auth=self.auth)
    def is_blocked(self):
        status = 'pass'
        for linked_issue in self.data['fields']['issuelinks']:
            if linked_issue['type']['name'] == 'Blocks' and linked_issue.get('inwardIssue'):
                linked_issue_status = linked_issue['inwardIssue']['fields']['status']['name'].lower()
                if linked_issue_status in ['failed', 'rejected']:
                    return 'fail'
                elif linked_issue_status not in ('done','resolved'):
                    status='blocked'
        return status
    def refresh(self):
        request = requests.get(self.data['self'],auth=self.auth)
        self.data = request.json()
    def get_issue_type(self):
        return self.data['fields']['issuetype']['name']
    def get_status(self):
        self.refresh()
        return self.data['fields']['status']['name'].upper()
   
    def get_attachment(self,filename):
        for attachment in self.get_attachments():
            if attachment.get_name() == filename:
                return attachment
        return {}
    def get_attachments(self):
        attachments = []
        request = requests.get(self.data["self"], auth=self.auth)
        if request.ok:
            for data in request.json()["fields"]["attachment"]:
                attachments.append(attachment(data, self.auth))
            return attachments
        else:
            raise Exception(
                f"{request.text}\nError in jira.client.issue.get_attachments"
            )
    def add_attachment(self, filename, content):
        headers = {"Accept": "application/json", "X-Atlassian-Token": "no-check"}
        request = requests.post(
            url=f'{self.data["self"]}/attachments',
            headers=headers,
            auth=self.auth,
            files={"file": (filename, content, "application-type")},
        )
        if request.ok:
            return attachment(request.json()[0], self.auth)
        else:
            raise Exception(
                f"{request.text}\nError in jira.client.issue.add_attachment"
            )
    def encrypt_all_attachments_with_kms_key(self, kms_client, key_arn):
        for attachment in self.get_attachments():
            if attachment.get_size() < 4096 and not attachment.get_name().endswith('.enc'):
                content = attachment.get_content()
                encrypted_content=kms_client.encrypt(KeyId=key_arn, Plaintext=content)['CiphertextBlob']
                self.add_attachment(f"{attachment.get_name()}.enc", encrypted_content)
                attachment.delete()
            else:
                print(f"File {attachment.get_name} too large to encrypt")            
        self.refresh()
    
    def get_field(self, key):
        field_metas = list(filter(lambda x: x["name"] == key, self.fields_metadata))
        if len(field_metas) == 0:
            raise Exception("Error in jira.client.issue.get_field (field not found)")
        field_values = []
        for field_meta in field_metas:
            query = {"fields": field_meta["id"]}
            request = requests.get(self.data["self"], auth=self.auth, params=query)
            if request.ok:
                field_data = request.json()["fields"][field_meta["id"]]
                if field_data:
                    if field_meta["schema"]["type"] == "array":
                        result = []
                        for item in field_data:
                            if field_meta["schema"]["items"] in ["option"]:
                                result.append(item["value"])
                            else:
                                result.append(item)
                        field_values.append(result)
                    elif field_meta["schema"]["type"] in ["option"]:
                        field_values.append(field_data["value"])
                    else:
                        field_values.append(field_data)
            else:
                raise Exception(f"{request.text}\nError in jira.client.issue.get_field")
        if field_values:
             return field_values[0]
        else:
            return None
    

    def update_field(self, key, value):
        field_meta = {}
        for field in self.fields_metadata:
            if field["name"] == key:
                field_meta = field
                break
        if field_meta != {}:
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            payload = json.dumps({"fields": {field_meta["id"]: value}})
            request = requests.put(
                url=self.data["self"], headers=headers, data=payload, auth=self.auth
            )
            if request.ok:
                pass
            else:
                raise Exception(
                    f"{request.text}\nError in jira.client.issue.update_field"
                )
        else:
            raise Exception("Error in jira.client.issue.update_field (field not found)")
    
    def update_service_desk_organizations(self,organizations):
        # Please ensure organization is added to the service desk        
        org_list = list(map(lambda x:x['id'], organizations))
        if org_list:
            self.update_field('Organizations',org_list)
        else:
            raise Exception("Error in jira.client.issue.add_service_desk_organization (organization not found)")
    def get_key(self):
        return self.data["key"]

    def transit_issue(self, transit_name):
        transition = {}
        transitions_request = requests.get(
            f'{self.data["self"]}/transitions', auth=self.auth
        )
        if transitions_request.ok:
            transitions = transitions_request.json()["transitions"]
            for item in transitions:
                if item["name"].upper() == transit_name.upper():
                    transition = item
        else:
            raise Exception(
                f"{transitions_request.text}\nError in jira.client.issue.transit_issue"
            )
        if transition == {}:
            raise Exception(
                "Error in jira.client.issue.transit_issue (transition not found)"
            )
        jpayload = {"transition": {"id": transition["id"]}}
        payload = json.dumps(jpayload)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        request = requests.post(
            f"{self.data['self']}/transitions",
            data=payload,
            headers=headers,
            auth=self.auth,
        )
        if request.ok:
            return {"status": "success"}
        else:
            raise Exception(f"{request.text}\nError in jira.client.issue.transit_issue")
        
    def get_comments(self):
        comments = []
        request = requests.get(f'{self.data["self"]}/comment',auth=self.auth)
        if request.ok:
            for comment in request.json()['comments']:
                comments.append(comment)
            return comments
        else:
            raise Exception(f"{request.text}\nError in jira.client.issue.get_comments")
        
    def add_comment(self, comment, style="paragraph", attachments=[], internal=False):
        if type(comment) is str:
            payload = {'body': self.format_document(comment, style)}
        elif type(comment) is dict:
            payload = comment
        if attachments:
            media_content=[]
            for attachment in attachments:
                media_content.append({
                "type": "media",
                "attrs": {
                    "id": attachment.get_media_id(),
                    "type": "file",
                    "collection": ""
                }})
            payload['body']['content'].append({
                "type": "mediaGroup",
                "content": media_content
            })
        if internal:
            payload['properties'] =[{'key':'sd.public.comment','value':{"internal": True}}]
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        request = requests.post(
            f"{self.data['self']}/comment",
            data=json.dumps(payload),
            headers=headers,
            auth=self.auth,
        )
        if request.ok:
            pass
        else:
            raise Exception(f"{request.text}\nError in jira.client.issue.add_comment")
        
    def update_comment(self, comment_id, comment, style="paragraph", attachments=[]):
        #payload = json.dumps({"body": self.format_document(comment, style)})
        if type(comment) == str:
            payload = {'body': self.format_document(comment, style) }
        elif type(comment) == dict:
            payload = comment
        if attachments:
            media_content=[]
            for attachment in attachments:
                media_content.append({
                "type": "media",
                "attrs": {
                    "id": attachment.get_media_id(),
                    "type": "file",
                    "collection": ""
                }})
            payload['body']['content'].append({
                "type": "mediaGroup",
                "content": media_content
            })
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        request = requests.put(
            f"{self.data['self']}/comment/{comment_id}",
            data=json.dumps(payload),
            headers=headers,
            auth=self.auth,
        )
        if request.ok:
            pass
        else:
            raise Exception(f"{request.text}\nError in jira.client.issue.update_comment")
        
    def update_summary(self,content):
        self.update_field('Summary',content)

    def update_field_area(self, field, content, style="paragraph", append=True):
        if append and self.get_field(field):
            doc = self.get_field(field)
            doc['content'].append(self.format_document(content=content,style=style)['content'][0])
        else:
            doc = self.format_document(content=content,style=style)
        self.update_field(
            field,
            doc,
        )
    def update_description(self, content, style="paragraph", append=True):
        if append and self.get_field('Description'):
            doc = self.get_field('Description')
            doc['content'].append(self.format_document(content=content,style=style)['content'][0])
        else:
            doc = self.format_document(content=content,style=style)
        self.update_field(
            "Description",
            doc,
        )

    def format_document(self, content, style="paragraph"):
        # Type paragraph, codeBlock
        # Content Tuple (Type, Str (Content))
        if style not in ["codeBlock", "paragraph"]:
            raise Exception(
                "Error in jira.client.issue.format_document: unsupported style"
            )
        doc = {"version": 1, "type": "doc", "content": []}
        doc["content"].append(
            {
                "type": style,
                "content": [{"type": "text", "text": content}],
            }
        )
        
        return doc
    
    def get_property_keys(self):
        request = requests.get(f'{self.data["self"]}/properties',auth=self.auth)
        if request.ok:
            return request.json()['keys']
        else:
            raise Exception(f"{request.text}\nError in jira.client.issue.get_property_keys")
        
    def get_property(self, key):
        request = requests.get(f'{self.data["self"]}/properties/{key}',auth=self.auth)
        if request.ok:
            return request.json()['value']
        else:
            if request.status_code == 404:
                return None
            else:
                raise Exception(f"{request.text}\nError in jira.client.issue.get_property")
    
    def set_property(self, key, value):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        payload = value
        request = requests.put(
            f"{self.data['self']}/properties/{key}",
            data=json.dumps(payload),
            headers=headers,
            auth=self.auth,
        )
        if request.ok:
            pass
        else:
            raise Exception(f"{request.text}\nError in jira.client.issue.set_property")
    def get_forms(self):
        headers = {"Accept": "application/json", "Content-Type": "application/json", "X-ExperimentalApi": "opt-in"}
        url = f'{self.get_forms_url()}/issue/{self.get_key()}/form'
        request = requests.get(url,auth=self.auth,headers=headers)
        if request.ok:
            return request.json()
        else:
            raise Exception(f"{request.text}\nError in jira.client.issue.get_forms")
    
    def get_form_detail(self,form):
        headers = {"Accept": "application/json", "Content-Type": "application/json", "X-ExperimentalApi": "opt-in"}
        url = f'{self.get_forms_url()}/issue/{self.get_key()}/form/{form["id"]}'
        request = requests.get(url,auth=self.auth,headers=headers)
        if request.ok:
            return request.json()
        else:
            raise Exception(f"{request.text}\nError in jira.client.issue.get_form_detail")
    
    def reopen_form(self,form):
        headers = {"Accept": "application/json", "Content-Type": "application/json", "X-ExperimentalApi": "opt-in"}
        url = f'{self.get_forms_url()}/issue/{self.get_key()}/form/{form["id"]}/action/reopen'
        request = requests.put(url,auth=self.auth,headers=headers)
        if request.ok:
            return request.json()
        else:
            raise Exception(f"{request.text}\nError in jira.client.issue.get_forms")
    
    def get_resolution(self):
        if self.get_field('Resolution'):
            return self.get_field('Resolution').get('name')
        else:
            return None