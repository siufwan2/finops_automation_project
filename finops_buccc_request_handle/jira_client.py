import requests
import json
import re
from jira_issue import issue

class client:
    def __init__(self, baseurl, auth):
        self.baseurl = baseurl
        self.auth = auth
        self.forms_url = f'https://api.atlassian.com/jira/forms/cloud/{self.get_cloud_id()}'
        self.fields = self.get_fields()
    
    def webhook_parser(self,event, allow_retry = False):
        try:
            body = json.loads(event.get("body"))
            result ={}
            result['issue_key'] = body['issue']['key']
            result['retry'] = event['headers'].get('x-atlassian-webhook-retry', '') != ''
            if not allow_retry and result['retry']:
                raise Exception('Retry is not allowed')
            return result
        except Exception as e:
            print('Error in jira_webhook_parser: ', e)
            return False
    
    def get_issue_type(self, name):
        issue_types_list = self.get_issue_types()
        try:
            return list(filter(lambda x: x["name"] == name, issue_types_list))[0]
        except Exception as e:
            return {}
    def get_issue_types(self):
        request = requests.get(
            f"{self.baseurl}/rest/api/3/issuetype", auth=self.auth
        )
        if request.ok:
            return request.json()
        else:
            raise Exception(f"{request.text}\nError in jira.client.get_issue_types")
    
    def get_organizations_from_project(self, project_key):
        servicedesk = self.get_service_desk(project_key)
        request = requests.get(
            f"{self.baseurl}/rest/servicedeskapi/servicedesk/{servicedesk['id']}/organization",
            auth=self.auth
        )
        if request.ok:
            result = request.json()
            while request.json()['isLastPage'] == False:
                request = requests.get(
                    f"{self.baseurl}/rest/servicedeskapi/servicedesk/{servicedesk['id']}/organization",
                    auth=self.auth,
                    params={"start": request.json()['start'] + 1}
                )
                result['values'].extend(request.json()['values'])
            return result['values']
        else:
            raise Exception(f"{request.text}\nError in jira.client.get_organization_from_project")
        
    def add_organization_to_project(self, project_key, organization):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        servicedesk_id = self.get_service_desk(project_key)['id']
        payload = json.dumps(
            {
                "organizationId": organization['id'],
            }
        )
        request = requests.post(
            url=f"{self.baseurl}/rest/servicedeskapi/servicedesk/{servicedesk_id}/organization", 
            auth=self.auth,
            data=payload,
            headers=headers
        )
        if request.ok:
            return 
        else:
            raise Exception(
                f"{request.text}\nError in jira.client.add_organization_to_project"
            )
        
    def add_service_desk_organization(self, name):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        payload = json.dumps(
            {
                "name": name            }
        )
        request = requests.post(
            url=f"{self.baseurl}/rest/servicedeskapi/organization",
            auth=self.auth,
            data=payload,
            headers=headers
        )
        if request.ok:
            return request.json()
        else:
            raise Exception(
                f"{request.text}\nError in jira.client.add_jsm_organization"
            )
    def get_service_desk_organization_members(self,org):
        request = requests.get(
            f"{self.baseurl}/rest/servicedeskapi/organization/{org['id']}/user",
            auth=self.auth
        )
        result = request.json()
        if request.ok:
            while request.json()['isLastPage'] == False:
                request = requests.get(
                    f"{self.baseurl}/rest/servicedeskapi/organization/{org['id']}/user",
                    auth=self.auth,
                    params={"start": request.json()['start'] + 1}
                )
                result['values'].extend(request.json()['values'])
            return request.json()['values']
        else:
            raise Exception(f"{request.text}\nError in jira.client.get_service_desk_organization_member")
    def remove_service_desk_organization_members(self,org,users):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        user_ids = list(map(lambda x:x['accountId'],users))
        payload = json.dumps(
            {
                "accountIds": user_ids
            }
        )
        request = requests.delete(
            url=f"{self.baseurl}/rest/servicedeskapi/organization/{org['id']}/user",
            auth=self.auth,
            data=payload,
            headers=headers
        )
        if request.ok:
            return
        else:
            raise Exception(
                f"{request.text}\nError in jira.client.remove_jsm_organization_member"
            )
    
    def add_service_desk_organization_members(self,org,users):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        user_ids = list(map(lambda x:x['accountId'],users))
        payload = json.dumps(
            {
                "accountIds": user_ids
            }
        )
        request = requests.post(
            url=f"{self.baseurl}/rest/servicedeskapi/organization/{org['id']}/user",
            auth=self.auth,
            data=payload,
            headers=headers
        )
        if request.ok:
            return
        else:
            raise Exception(
                f"{request.text}\nError in jira.client.add_jsm_organization_member"
            )
    def get_service_desk_organization(self, name):
        request = requests.get(
            f"{self.baseurl}/rest/servicedeskapi/organization",
            auth=self.auth,
        )
        result = request.json()
        while request.json()['isLastPage'] == False:
            request = requests.get(
                f"{self.baseurl}/rest/servicedeskapi/organization",
                auth=self.auth,
                params={"start": request.json()['start'] + 50}
            )
            result['values'].extend(request.json()['values'])

        if request.ok:
           service_desk_org_list = list(filter(lambda x: x.get('name','') == name, result['values']))
           if len(service_desk_org_list) == 1:
                return service_desk_org_list[0]
           else:
                return {}
        else:
            raise Exception(f"{request.text}\nError in jira.client.get_service_desk_organization")            
    
    def add_issue_type(self, name, description, type="standard"):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        payload = json.dumps(
            {
                "name": name,
                "description": description,
                "type": type,
            }
        )
        request = requests.post(
            url=f"{self.baseurl}/rest/api/3/issuetype",
            auth=self.auth,
            data=payload,
            headers=headers,
        )
        if request.ok:
            return request.json()
        else:
            raise Exception(
                f"{request.text}\nError in jira.client.create_issue_type"
            )

    def get_user(self, email=None, accountId=None):
        query = {}
        if accountId != None:
            query["accountId"] = accountId
        elif email != None:
            query["query"] = email
        else:
            raise Exception(
                "Error in jira.client.get_user (email and accountId not provided)"
            )
        headers = {"Accept": "application/json"}
        request = requests.get(
            f"{self.baseurl}/rest/api/3/user/search",
            auth=self.auth,
            params=query,
            headers=headers,
        )
        if request.ok:
            for user in request.json():
                if email != None and user.get("emailAddress", "") == email:
                    return user
                elif accountId != None and user.get("accountId", "") == accountId:
                    return user
            return {}
        else:
            raise Exception(f"{request.text}\nError in jira.client.get_user")

    def get_user_groups(self, user):
        headers = {"Accept": "application/json"}
        query = {"accountId": user["accountId"]}
        request = requests.get(
            f"{self.baseurl}/rest/api/3/user/groups",
            headers=headers,
            auth=self.auth,
            params=query,
        )
        if request.ok:
            return request.json()
        else:
            raise Exception("Error in jira.client.get_user_groups")
    def get_groups(self, prefix=None):
        query = {"maxResults": 10000}
        if prefix:
            query["query"] = prefix
        request = requests.get(
            f"{self.baseurl}/rest/api/3/groups/picker", auth=self.auth, params=query
        )
        if request.ok:
            return request.json()['groups']
        else:
            raise Exception(f"{request.text}\nError in jira.client.get_groups")
    def get_group(self, group_name):
        query = {"query": group_name}
        request = requests.get(
            f"{self.baseurl}/rest/api/3/groups/picker", auth=self.auth, params=query
        )
        if request.ok:
            for group in request.json()["groups"]:
                if group["name"] == group_name:
                    return group
            return {}
        else:
            raise Exception(f"{request.text}\nError in jira.client.get_groups")

    def add_group(self, group_name):
        if self.get_group(group_name=group_name) != {}:
            print(f"Group {group_name} already exist")
            return self.get_group(group_name=group_name)
        else:
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            payload = json.dumps({"name": group_name})
            request = requests.post(
                f"{self.baseurl}/rest/api/3/group",
                auth=self.auth,
                headers=headers,
                data=payload,
            )
            if request.ok:
                return request.json()
            else:
                raise Exception(f"{request.text}\nError in jira.client.add_group")

    def remove_group(self, group):
            query = {"groupId": group["groupId"]}
            request = requests.delete(
                f"{self.baseurl}/rest/api/3/group", auth=self.auth, params=query
            )
            if request.ok:
                return True
            else:
                raise Exception(f"{request.text}\nError in jira.client.remove_group")

    def get_group_member(self, group):
        group_id = group["groupId"]
        query = {"groupId": group_id}
        request = requests.get(
            f"{self.baseurl}/rest/api/3/group/member", auth=self.auth, params=query
        )
        result = request.json()
        while request.json()["isLast"] == False:
            request = requests.get(
                f"{self.baseurl}/rest/api/3/group/member",
                auth=self.auth,
                params={"startAt": request.json()["start"] + 1},
            )
            result["values"].extend(request.json()["values"])
        if request.ok:
            return result["values"]
        else:
            raise Exception(f"{request.text}\nError in jira.client.get_group_memeber")

    def add_group_member(self, group, user):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        group_id = group["groupId"] if type(group) == dict else group
        query = {"groupId": f"{group_id}"}
        user_id = user["accountId"] if type(user) == dict else user
        payload = json.dumps({"accountId": user_id})
        request = requests.post(
            f"{self.baseurl}/rest/api/3/group/user",
            auth=self.auth,
            params=query,
            data=payload,
            headers=headers,
        )
        if request.ok:
            return request.json()
        else:
            raise Exception(f"{request.text}\nError in jira.client.add_group_member")

    def remove_group_member(self, group, user):
        group_id = group["groupId"]
        query = {"groupId": group_id, "accountId": user["accountId"]}
        request = requests.delete(
            f"{self.baseurl}/rest/api/3/group/user", auth=self.auth, params=query
        )
        if request.ok:
            pass
        else:
            raise Exception(f"{request.text}\nError in jira.client.remove_group_member")
        
    def add_issue(self,project_key,issue_type, summary, description=None, fields=None, request_type=None, reporter=None):
        if fields:
            jira_fields = { self.get_field(key)['id']:value for key,value in fields.items() }
        else:
            jira_fields = {}
        if request_type:
            request_type_number = self.get_request_type(project_key=project_key, name=request_type,issue_type=issue_type)['id']
            if request_type_number:
                jira_fields[self.get_field('Request Type')['id']] = f'{request_type_number}'
        if reporter:
            jira_fields['reporter'] = {'id':reporter['accountId']}
        jira_fields['summary'] = summary
        if description:
            jira_fields['description'] = self.format_document(description)
        jira_fields['project'] = {'key':project_key}
        jira_fields['issuetype'] = {'id':self.get_issue_type(issue_type)['id']}
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        payload = json.dumps(
            {'fields': jira_fields})
        request = requests.post(
            url=f"{self.baseurl}/rest/api/3/issue",
            auth=self.auth,
            data=payload,
            headers=headers,
        )
        if request.ok:
            return self.get_issue(request.json()['key'])
        else:
            raise Exception(
                f"{request.text}\nError in jira.client.add_issue"
            )
        
    def get_issue(self, issue_key):
        fields_metadata = self.get_fields()
        request = requests.get(
            f"{self.baseurl}/rest/api/3/issue/{issue_key}", auth=self.auth
        )

        if request.ok:
            temp_issue = issue(
                data=request.json(), auth=self.auth, fields_metadata=fields_metadata
            )
            return temp_issue
        else:
            raise Exception(f"{request.text}\nError in jira.client.get_issue")

    def get_issues(self, jql):
        issues = []
        query = {"jql": f"{jql} ORDER BY created ASC", "expand": "names,transitions"}
        request = requests.get(
            f"{self.baseurl}/rest/api/3/search", auth=self.auth, params=query
        )
        if request.ok:
            fields_metadata = self.get_fields()
            for data in request.json()["issues"]:
                temp_issue = issue(data=data, auth=self.auth, fields_metadata=fields_metadata)
                issues.append(
                    temp_issue
                )
            return issues
        else:
            raise Exception(f"{request.text}\nError in jira.client.get_issue")

    def get_field_default_option(self, field):
        field_id = field["id"]
        default_context_request = requests.get(
            f"{self.baseurl}/rest/api/3/field/{field_id}/context/defaultValue",
            auth=self.auth,
        )
        if default_context_request.ok:
            default_context_id = default_context_request.json()["values"][0][
                "contextId"
            ]
        else:
            raise Exception(
                "Error in jira.client.get_field_default_option (default_context_request)"
            )
        request = requests.get(
            f"{self.baseurl}/rest/api/3/field/{field_id}/context/{default_context_id}/option",
            auth=self.auth,
        )
        if request.ok:
            return request.json()["values"]
        else:
            raise Exception(
                f"{request.text}\nError in jira.client.get_field_default_option"
            )

    def add_field_default_option(self, field, value):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        field_id = field["id"]
        default_context_request = requests.get(
            f"{self.baseurl}/rest/api/3/field/{field_id}/context/defaultValue",
            auth=self.auth,
        )
        if default_context_request.ok:
            default_context_id = default_context_request.json()["values"][0][
                "contextId"
            ]
        else:
            raise Exception(
                "Error in jira.client.add_field_default_option (default_context_request)"
            )
        payload = json.dumps({"options": [{"value": value}]})
        request = requests.post(
            f"{self.baseurl}/rest/api/3/field/{field_id}/context/{default_context_id}/option",
            headers=headers,
            data=payload,
            auth=self.auth,
        )
        if request.ok:
            pass
        else:
            raise Exception(
                f"{request.text}\nError in jira.client.add_field_default_option"
            )

    def remove_field_default_option(self, field, value):
        existing_option = self.get_field_default_option(field=field)
        option_id = ""
        for option in existing_option:
            if option["value"] == value:
                option_id = option["id"]
        if option_id:
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            field_id = field["id"]
            default_context_request = requests.get(
                f"{self.baseurl}/rest/api/3/field/{field_id}/context/defaultValue",
                auth=self.auth,
            )
            if default_context_request.ok:
                default_context_id = default_context_request.json()["values"][0][
                    "contextId"
                ]
            else:
                raise Exception(
                    "Error in jira.client.remove_field_default_option (default_context_request)"
                )
            request = requests.delete(
                f"{self.baseurl}/rest/api/3/field/{field_id}/context/{default_context_id}/option/{option_id}",
                auth=self.auth,
            )
            if request.ok:
                pass
            else:
                raise Exception(f"Error in jira.client.remove_field_default_option")
        else:
            raise Exception(
                f"Error in jira.client.remove_field_default_option\nOption not found for value({value})"
            )
    def get_service_desk(self, projectKey):
        request = requests.get(
            f"{self.baseurl}/rest/servicedeskapi/servicedesk",
            auth=self.auth,
        )
        result = request.json()
        while request.json()['isLastPage'] == False:
            request = requests.get(
                f"{self.baseurl}/rest/servicedeskapi/servicedesk",
                auth=self.auth,
                params={"start": request.json()['start'] + 1}
            )
            result['values'].extend(request.json()['values'])
        if request.ok:
            for desk in result["values"]:
                if desk["projectKey"] == projectKey:
                    return desk
            return {}
        else:
            raise Exception(f"{request.text}\nError in jira.client.get_service_desk")
    def remove_request_type(self, project_key, request_type):
        headers = {"Accept": "application/json", "Content-Type": "application/json","X-ExperimentalApi": "opt-in"}
        desk = self.get_service_desk(project_key)
        request = requests.delete(
            f"{self.baseurl}/rest/servicedeskapi/servicedesk/{desk['id']}/requesttype/{request_type['id']}",
            auth=self.auth,
            headers=headers
        )
        if request.ok:
            return
        else:
            raise Exception(f"{request.text}\nError in jira.client.remove_request_type")
    def add_request_type(self, project_key, name, issue_type, description='', help_text=""):
        desk = self.get_service_desk(project_key)
        headers = {"Accept": "application/json", "Content-Type": "application/json","X-ExperimentalApi": "opt-in"}
        payload = json.dumps(
            {
                "name": name,
                "description": description,
                "helpText": help_text,
                "issueTypeId": self.get_issue_type(issue_type)['id'],
            }
        )
        request = requests.post(
            f"{self.baseurl}/rest/servicedeskapi/servicedesk/{desk['id']}/requesttype",
            auth=self.auth,
            headers=headers,
            data=payload,
        )
        if request.ok:
            return request.json()
        else:
            raise Exception(f"{request.text}\nError in jira.client.add_request_type")
    def get_request_types(self, project_key):
        desk = self.get_service_desk(project_key)
        request = requests.get(
            f"{self.baseurl}/rest/servicedeskapi/servicedesk/{desk['id']}/requesttype",
            auth=self.auth,
        )
        result = request.json()
        while request.json()['isLastPage'] == False:
            request = requests.get(
                f"{self.baseurl}/rest/servicedeskapi/servicedesk/{desk['id']}/requesttype",
                auth=self.auth,
                params={"start": request.json()['start'] + 1}
            )
            result['values'].extend(request.json()['values'])
        if request.ok:
            return result['values']
        else:
            raise Exception(f"{request.text}\nError in jira.client.get_request_types")
    def get_request_type_field(self, request_type):
        params = {'expand': 'hiddenFields'}
        request = requests.get(
            f"{request_type['_links']['self']}/field",
            auth=self.auth,
            params=params
        )
        if request.ok:
            return request.json()
        else:
            raise Exception(f"{request.text}\nError in jira.client.get_request_type_field")

    def get_request_type(self, project_key, name, issue_type=None):
        request_types = self.get_request_types(project_key)
        request_types = list(filter(lambda x: x.get("name",'') == name, request_types))
        if issue_type:
            request_types = list(filter(lambda x: x.get("issueTypeId",'') == self.get_issue_type(issue_type)['id'], request_types))
        if len(request_types) == 0:
            return None
        else:
            return request_types[0]

    
    # Used to create service request in JSM
    def create_request(self, project_key, request_type_name, form):
        request_type = self.get_request_type(project_key=project_key, name=request_type_name)
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        payload = json.dumps({
                "serviceDeskId": request_type["serviceDeskId"],
                "requestTypeId": request_type["id"],
                "form": form, 
                "requestFieldValues":{}
            })
    
        request = requests.post(
            f"{self.baseurl}/rest/servicedeskapi/request",
            auth=self.auth,
            headers=headers,
            data=payload,
        )
        if request.ok:
            return request.json()
        else:
            raise Exception(f"{request.text}\nError in jira.client.create_request")
        
    def get_fields(self):
        request = requests.get(url=f"{self.baseurl}/rest/api/3/field", auth=self.auth)
        if request.ok:
            return request.json()
        else:
            raise Exception(f"{request.text}\nError in jira.client.get_fields")

    def get_field(self, name, type=None):
        field = list(filter(lambda x: x["name"] == name, self.fields))
        if len(field) == 0:
            return {}
        if type is None:
            return field[0]
        else:
            try:
                return list(filter(lambda x: x["schema"]["custom"] == f'com.atlassian.jira.plugin.system.customfieldtypes:{type.lower()}', field))[0]
            except:
                return {}

    def add_field(self, name, type, description=""):
        selectorname = {
            "cascadingselect": "cascadingselectsearcher",
            "datepicker": "daterange",
            "datetime": "datetimerange",
            "float": "exactnumber",
            "grouppicker": "grouppickersearcher",
            "importid": "exactnumber",
            "labels": "labelsearcher",
            "multicheckboxes": "multiselectsearcher",
            ## Wrong API Ref found in jira 
            "multigrouppicker": "grouppickersearcher",
            "multiselect": "multiselectsearcher",
            "multiuserpicker": "userpickergroupsearcher",
            "multiversion": "versionsearcher",
            "project": "projectsearcher",
            "radiobuttons": "multiselectsearcher",
            "readonlyfield": "textsearcher",
            "select": "multiselectsearcher",
            "textarea": "textsearcher",
            "textfield": "textsearcher",
            "url": "exacttextsearcher",
            "userpicker": "userpickergroupsearcher",
            "version": "versionsearcher",
        }
        if self.get_field(name, type):
            raise Exception(f"Field Name {name} already exist")
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        payload = json.dumps(
            {
                "name": name,
                "description": description,
                "type": f"com.atlassian.jira.plugin.system.customfieldtypes:{type}",
                "searcherKey": f"com.atlassian.jira.plugin.system.customfieldtypes:{selectorname[type]}",
            }
        )
        request = requests.post(
            url=f"{self.baseurl}/rest/api/3/field",
            auth=self.auth,
            data=payload,
            headers=headers,
        )
        if request.ok:
            self.fields = self.get_fields()
            return request.json()
        else:
            raise Exception(f"Error in jira.client.add_field\n{request.text}")

    def update_field(self, field, name, description=""):
        selectorname = {
            "cascadingselect": "cascadingselectsearcher",
            "datepicker": "daterange",
            "datetime": "datetimerange",
            "float": "exactnumber",
            "grouppicker": "grouppickersearcher",
            "importid": "exactnumber",
            "labels": "labelsearcher",
            "multicheckboxes": "multiselectsearcher",
            "multigrouppicker": "grouppickersearcher",
            "multiselect": "multiselectsearcher",
            "multiuserpicker": "userpickergroupsearcher",
            "multiversion": "versionsearcher",
            "project": "projectsearcher",
            "radiobuttons": "multiselectsearcher",
            "readonlyfield": "textsearcher",
            "select": "multiselectsearcher",
            "textarea": "textsearcher",
            "textfield": "textsearcher",
            "url": "exacttextsearcher",
            "userpicker": "userpickergroupsearcher",
            "version": "versionsearcher",
        }
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        type = field["schema"]["custom"].split(":")[-1]
        payload = json.dumps(
            {
                "description": description,
                "name": name,
                "searcherKey": f"com.atlassian.jira.plugin.system.customfieldtypes:{selectorname[type]}",
            }
        )
        request = requests.put(
            url=f"{self.baseurl}/rest/api/3/field/{field['id']}",
            auth=self.auth,
            data=payload,
            headers=headers,
        )
        if request.ok:
            pass
        else:
            raise Exception(f"Error in jira.client.update_field\n{request.text}")
        
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
    def get_field_configuration(self, name):
        response = requests.get(
            f"{self.baseurl}/rest/api/3/fieldconfiguration",
            auth=self.auth,
        )
        result = response.json()
        while response.json()['isLast'] == False:
            response = requests.get(
                f"{self.baseurl}/rest/api/3/field/fieldconfiguration",
                auth=self.auth,
                params={"startAt": response.json()['startAt'] + response.json()['maxResults']}
            )
            result['values'].extend(response.json()['values'])
        if response.ok:
            result = list(filter(lambda x: x['name'] == name, result['values']))
            if len(result) == 1:
                return result[0]
            else:
                return {}
        else:
            raise Exception(f"{response.text}\nError in jira.client.get_field_config")

    def add_field_configuration(self,name, description=""):
        if self.get_field_configuration(name):
            print(f"Field Configuration {name} already exist")
            return self.get_field_configuration(name)
        else:
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            payload = json.dumps(
                {
                    "name": name,
                    "description": description,
                }
            )
            response = requests.post(
                url=f"{self.baseurl}/rest/api/3/fieldconfiguration",
                auth=self.auth,
                data=payload,
                headers=headers,
            )
            if response.ok:
                return response.json()
            else:
                raise Exception(f"Error in jira.client.add_field_configuration\n{response.text}")

    def update_field_configuration_items(self, field_config, fields):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        payload = {
            'fieldConfigurationItems':[]
        }
        for field in fields:
            payload['fieldConfigurationItems'].append({
                'id':self.get_field(field)['id']
            })
        resposne = requests.put(
            url=f"{self.baseurl}/rest/api/3/fieldconfiguration/{field_config['id']}/fields",
            auth=self.auth,
            data=json.dumps(payload),
            headers=headers,
        )
        if resposne.ok:
            pass
        else:
            raise Exception(f"Error in jira.client.update_field_configuration_items\n{resposne.text}")
    
    def get_field_configuration_items(self, field_config):
        response = requests.get(
            f"{self.baseurl}/rest/api/3/fieldconfiguration/{field_config['id']}/fields",
            auth=self.auth,
        )
        result = response.json()
        while response.json()['isLast'] == False:
            response = requests.get(
                f"{self.baseurl}/rest/api/3/fieldconfiguration/{field_config['id']}/fields",
                auth=self.auth,
                params={"startAt": response.json()['startAt'] + response.json()['maxResults']}
            )
            result['values'].extend(response.json()['values'])
        if response.ok:
            return result['values']
        else:
            raise Exception(f"Error in jira.client.get_field_configuration_items\n{response.text}")
    def get_screens(self):
        response = requests.get(
            f"{self.baseurl}/rest/api/3/screens",
            auth=self.auth,
        )
        result = response.json()
        while response.json()['isLast'] == False:
            response = requests.get(
                f"{self.baseurl}/rest/api/3/screens",
                auth=self.auth,
                params={"startAt": response.json()['startAt'] + response.json()['maxResults']}
            )
            result['values'].extend(response.json()['values'])
        if response.ok:
           return result['values']
        else:
            raise Exception(f"{response.text}\nError in jira.client.get_screens")
            
    def get_screen(self, name):
        all_screen = self.get_screens()
        result = list(filter(lambda x: x['name'] == name, all_screen))
        if len(result) == 1:
            return result[0]
        else:
            return {}
    
    def add_screen(self,name, description=""):
        screen = self.get_screen(name)
        if screen:
            print(f"Screen {name} already exist")
            return screen
        else:
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            payload = json.dumps(
                {
                    "name": name,
                    "description": description,
                }
            )
            response = requests.post(
                url=f"{self.baseurl}/rest/api/3/screens",
                auth=self.auth,
                data=payload,
                headers=headers,
            )
            if response.ok:
                return response.json()
            else:
                raise Exception(f"Error in jira.client.add_screen\n{response.text}")
    
    def remove_screen(self, screen):
        response = requests.delete(
            f"{self.baseurl}/rest/api/3/screens/{screen['id']}",
            auth=self.auth,
        )
        if response.ok:
            return True
        else:
            raise Exception(f"Error in jira.client.remove_screen\n{response.text}")

    def get_screen_tabs(self, screen):
        response = requests.get(
            f"{self.baseurl}/rest/api/3/screens/{screen['id']}/tabs",
            auth=self.auth,
        )
        result = response.json()
        if response.ok:
            return result
        else:
            raise Exception(f"{response.text}\nError in jira.client.get_screen_tabs")
        
    def get_screen_tab(self, screen, name):
        all_tab = self.get_screen_tabs(screen)
        result = list(filter(lambda x: x['name'] == name, all_tab))
        if len(result) == 1:
            return result[0]
        else:
            return {}
        
    def add_screen_tab(self,screen,name):
        if self.get_screen_tab(screen,name):
            print(f"Screen Tab {name} already exist")
            return self.get_screen_tab(screen,name)
        else:
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            payload = json.dumps(
                {
                    "name": name
                                    }
            )
            response = requests.post(
                url=f"{self.baseurl}/rest/api/3/screens/{screen['id']}/tabs",
                auth=self.auth,
                data=payload,
                headers=headers,
            )
            if response.ok:
                return response.json()
            else:
                raise Exception(f"Error in jira.client.add_screen_tab\n{response.text}")
            
    def remove_screen_tab(self, screen, tab):
        response = requests.delete(
            f"{self.baseurl}/rest/api/3/screens/{screen['id']}/tabs/{tab['id']}",
            auth=self.auth,
        )
        if response.ok:
            return True
        else:
            raise Exception(f"Error in jira.client.remove_screen_tab\n{response.text}")
                            
    def get_screen_tab_fields(self, screen , tab ):
        response = requests.get(
            f"{self.baseurl}/rest/api/3/screens/{screen['id']}/tabs/{tab['id']}/fields",
            auth=self.auth,
        )
        result = response.json()
        if response.ok:
            return result
        else:
            raise Exception(f"Error in jira.client.get_screen_tab_field\n{response.text}")
    
    def get_screen_tab_field(self, screen , tab, field):
        fields = self.get_screen_tab_fields(screen,tab)
        result = list(filter(lambda x: x['id'] == field['id'], fields))
        if len(result) == 1:
            return result[0]
        else:
            return {}
        
    def add_screen_tab_field(self,screen,tab,field):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        payload = json.dumps(
            {
                "fieldId": field['id']
                                        }
        )
        response = requests.post(
            url=f"{self.baseurl}/rest/api/3/screens/{screen['id']}/tabs/{tab['id']}/fields",
            auth=self.auth,
            data=payload,
            headers=headers,
        )
        if response.ok:
            return response.json()
        else:
            raise Exception(f"Error in jira.client.add_screen_tab_field\n{response.text}")
    
    def remove_screen_tab_field(self, screen, tab, field):
        response = requests.delete(
            f"{self.baseurl}/rest/api/3/screens/{screen['id']}/tabs/{tab['id']}/fields/{field['id']}",
            auth=self.auth,
        )
        if response.ok:
            return True
        else:
            raise Exception(f"Error in jira.client.remove_screen_tab_field\n{response.text}")

    def get_cloud_id(self):
        response = requests.get(
            f"{self.baseurl}/_edge/tenant_info",
            auth=self.auth,
        )
        if response.ok:
            return response.json()["cloudId"]
        else:
            raise Exception(f"Error in jira.client.get_cloud_id\n{response.text}")

    def get_forms(self, project_key):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-ExperimentalApi": "opt-in",
        }
        response = requests.get(
            f"{self.forms_url}/project/{project_key}/form",
            auth=self.auth,
            headers=headers,
        )
        result = response.json()
        if response.ok:
            return result
        else:
            raise Exception(f"Error in jira.client.get_forms\n{response.text}")

    def get_form(self, project_key, name):
        forms = self.get_forms(project_key)
        result = list(filter(lambda x: x["name"] == name, forms))
        if len(result) == 1:
            id = result[0]["id"]
            headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-ExperimentalApi": "opt-in",
            }
            response = requests.get(
                f"{self.forms_url}/project/{project_key}/form/{id}",
                auth=self.auth,
                headers=headers,
            )
            return response.json()
        else:
            return {}

    def add_form(self, project_key, name, fields=None, lock=True, pdf=False, design=None):
        form = self.get_form(project_key, name)
        if form:
            print(f"Form {name} already exist")
            return form
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-ExperimentalApi": "opt-in",
        }
        if design:
            payload = {
                "design": design,
            }
        elif fields:
            payload = {
                "design": self.generate_form_design(name,fields, lock=True, pdf=False),
            }
        else:
            raise Exception("Error in jira.client.add_form: fields or design not provided")
        response = requests.post(
            f"{self.forms_url}/project/{project_key}/form",
            auth=self.auth,
            data=json.dumps(payload),
            headers=headers,
        )
        result = response.json()
        if response.ok:
            return result
        else:
            raise Exception(f"Error in jira.client.add_form\n{response.text}")
        
    def update_form(self, project_key, name, fields=None, lock=True, pdf=False, design=None):
        form = self.get_form(project_key, name)
        if not form:
            print('Form not found, Now creating')
            form = self.add_form(project_key, name, fields, lock, pdf, design)
            return form
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-ExperimentalApi": "opt-in",
        }
        if design:
            payload = {
                "design": design,
            }
        elif fields:
            payload = {
                "design": self.generate_form_design(name,fields, lock=True, pdf=False),
            }
        else:
            raise Exception("Error in jira.client.add_form: fields or design not provided")
        if form.get('publish'):
            payload['publish'] = form['publish']
        response = requests.put(
            f"{self.forms_url}/project/{project_key}/form/{form['id']}",
            auth=self.auth,
            data=json.dumps(payload),
            headers=headers,
        )
        result = response.json()
        if response.ok:
            return result
        else:
            raise Exception(f"Error in jira.client.update_form\n{response.text}")

    def generate_form_design(self,name,fields, lock=True, pdf=False):
        design = {
            "conditions": {},
            "layout": [
                {
                "version": 1,
                "type": "doc",
                "content": []
                }
            ],
            "questions": {},
            "sections": {},
            "settings": {
                "name": name,
                "language": "en",
                "submit": {
                    "lock": lock,
                    "pdf": pdf
                }
            }
        }
        type_mapping = {
            'Dropdown':'cd',
            'Paragraph':'pg',
            'Email': 'te',
            'LongText':'tl',
            'ShortText': 'ts',
            'Attachment': 'at',
            'MultipleUser': 'um',
            'DateTime': 'dt',
        }
        for field in fields:
            if field.get('label'):
                index = fields.index(field) + 1
                design['questions'][f"{index}"] = {
                    "description": field.get('description',""),
                    "label": field['label'],
                    "questionKey": field.get('description',""),
                    "type": type_mapping[field['type']],
                    "validation": {
                        "rq": field.get('required',False)
                    }
                }
                if field.get('regx'):
                    design['questions'][f"{index}"]["validation"]["rgx"] = {'p': field['regx']['pattern'], 'm': field['regx']['message']}
                if field.get('jira_field'):
                    if field['type'] in ('Dropdown'):
                        design['questions'][f"{index}"]["jiraField"] = self.get_field(field['jira_field'],'select')['id']
                    elif field['type'] in ('LongText','ShortText'):
                        design['questions'][f"{index}"]["jiraField"] = self.get_field(field['jira_field'],'textfield')['id']
                    elif field['type'] in ('DateTime'):
                        design['questions'][f"{index}"]["jiraField"] = self.get_field(field['jira_field'],'datetime')['id']
                    elif field['type'] in ('MultipleUser'):
                        design['questions'][f"{index}"]["jiraField"] = self.get_field(field['jira_field'],'multiuserpicker')['id']
                    else:
                        design['questions'][f"{index}"]["jiraField"] = self.get_field(field['jira_field'])['id']
                    if field.get('default'):
                        if field['type'] in ('Dropdown'):
                            field_object = self.get_field(field['jira_field'])
                            option_list = self.get_field_default_option(field_object)
                            try:
                                option_id = list(filter(lambda x: x['value'] == field['default'], option_list))[0]['id']
                                design['questions'][f"{index}"]["defaultAnswer"] = {'choices': [option_id]}
                            except IndexError:
                                print(f"Option {field['default']} not found in {field['name']},skip")
                        elif field['type'] in ('LongText','ShortText','Paragraph'):
                            design['questions'][f"{index}"]["defaultAnswer"] = {'text': field['default']}
                        else:
                            print(f"Default value for {field['type']} not supported, skip")
                design['layout'][0]['content'].append({
                            "type": "extension",
                            "attrs": {
                                "extensionKey": "question",
                                "extensionType": "com.thinktilt.proforma",
                                "layout": "default",
                                "parameters": {
                                    "id": index
                                }
                            }})
            elif field.get('content'):
                design['layout'][0]['content'].append(self.format_document(field['content'],style=field.get('style','paragraph'))['content'][0])
        return design
    def get_workflow(self,name):
        workflows = self.get_workflows()
        result = list(filter(lambda x: x['id']['name'] == name, workflows))
        if len(result) == 1:
            return result[0]
        else:
            return {}
    def get_workflows(self):
        params = {'expand': 'transitions,statuses'}
        response = requests.get(
            f"{self.baseurl}/rest/api/3/workflow/search",
            auth=self.auth,
            params=params
        )
        result = response.json()
        if response.ok:
            while response.json()['isLast'] == False:
                response = requests.get(
                    f"{self.baseurl}/rest/api/3/workflow/search",
                    auth=self.auth,
                    params=params | {"startAt": response.json()['startAt'] + response.json()['maxResults']}
                )
                result['values'].extend(response.json()['values'])
            return result['values']
        else:
            raise Exception(f"Error in jira.client.get_workflows\n{response.text}")
    def get_project(self,key):
        response = requests.get(
            f"{self.baseurl}/rest/api/3/project/{key}",
            auth=self.auth
        )
        if response.ok:
            return response.json()
        else:
            raise Exception(f"Error in jira.client.get_project\n{response.text}")
        
    def get_issue_type_scheme(self,project):
        params = {'projectId': self.get_project(project)['id']}
        response = requests.get(
            f"{self.baseurl}/rest/api/3/issuetypescheme/project",
            auth=self.auth,
            params=params
        )
        if response.ok:
            if response.json()['values']:
                return response.json()['values'][0]
            else:
                return None
        else:
            raise Exception(f"Error in jira.client.get_issue_type_schemes\n{response.text}")
    def get_issue_type_from_scheme(self,scheme):
        response = requests.get(
            f"{self.baseurl}/rest/api/3/issuetypescheme/mapping",
            auth=self.auth
        )
        result= response.json()
        if response.ok:
            scheme_id = scheme['issueTypeScheme']['id']
            while response.json()['isLast'] == False:
                response = requests.get(
                    f"{self.baseurl}/rest/api/3/issuetypescheme/mapping",
                    auth=self.auth,
                    params={"startAt": response.json()['startAt'] + response.json()['maxResults']}
                )
                result['values'].extend(response.json()['values'])
            return list(map(lambda x:x['issueTypeId'],(filter(lambda x: x['issueTypeSchemeId'] == scheme_id, result['values']))))
        else:
            raise Exception(f"Error in jira.client.get_issue_type_from_scheme\n{response.text}")
        
    def remove_issue_type_from_scheme(self,scheme, issue_type_ids):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        for issue_type_id in issue_type_ids:
            response = requests.delete(
                f"{self.baseurl}/rest/api/3/issuetypescheme/{scheme['issueTypeScheme']['id']}/issuetype/{issue_type_id}",
                auth=self.auth,
                headers=headers
            )
            if response.ok:
                print(f"Issue Type {issue_type_id} removed")
            else:
                raise Exception(f"Error in jira.client.remove_issue_type_scheme\n{response.text}")
        return True
        
    def assign_issue_type_to_scheme(self,scheme,issue_type_list):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        existing_issue_type = self.get_issue_type_from_scheme(scheme)
        pending_issue_type = [ issue_type['id'] for issue_type in issue_type_list if issue_type['id'] not in existing_issue_type]
        print(f"Issue Type will be added: {pending_issue_type}")
        if not pending_issue_type:
            return True
        payload = json.dumps(
            {
                'issueTypeIds': [ issue_type['id'] for issue_type in issue_type_list if issue_type['id'] not in existing_issue_type]
            }
        )
        response = requests.put(
            f"{self.baseurl}/rest/api/3/issuetypescheme/{scheme['issueTypeScheme']['id']}/issuetype",
            auth=self.auth,
            data=payload,
            headers=headers
        )
        if response.ok:
            return True
        else:
            raise Exception(f"Error in jira.client.assign_issue_type_scheme\n{response.text}")
    def get_screen_schemes(self):
        response = requests.get(
            f"{self.baseurl}/rest/api/3/screenscheme",
            auth=self.auth,
        )
        if response.ok:
            result = response.json()
            while response.json()['isLast'] == False:
                response = requests.get(
                    f"{self.baseurl}/rest/api/3/screenscheme",
                    auth=self.auth,
                    params={ "startAt": response.json()['startAt'] + response.json()['maxResults']}
                )
                result['values'].extend(response.json()['values'])
            return result['values']
        else:
            raise Exception(f"Error in jira.client.get_screen_scheme\n{response.text}")
        
    def get_screen_scheme(self,name):
        schemes = self.get_screen_schemes()
        result = list(filter(lambda x: x['name'] == name, schemes))
        if len(result) == 1:
            return result[0]
        else:
            return {}
        
    def add_screen_scheme(self, name, default_screen, create_screen=None, edit_screen=None, view_screen=None, description=""):
        if self.get_screen_scheme(name):
            print(f"Screen Scheme {name} already exist")
            return self.get_screen_scheme(name)
        else:
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            payload = {
                    "name": name,
                    "description": description,
                    "screens": {
                        "default": default_screen['id']
                    }
                }
            if create_screen:
                payload['screens']['create'] = create_screen['id']
            if edit_screen:
                payload['screens']['edit'] = edit_screen['id']
            if view_screen:
                payload['screens']['view'] = view_screen['id']
            response = requests.post(
                url=f"{self.baseurl}/rest/api/3/screenscheme",
                auth=self.auth,
                data=json.dumps(payload),
                headers=headers,
            )
            if response.ok:
                return response.json()
            else:
                raise Exception(f"Error in jira.client.add_screen_scheme\n{response.text}")
    
    def update_screen_scheme(self, scheme, default_screen=None, create_screen=None, edit_screen=None, view_screen=None, description=""):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        payload = {
                "name": scheme['name'],
                "description": description,
                "screens": {}
            }
        if default_screen:
            payload['screens']['default'] = default_screen['id']
        if create_screen:
            payload['screens']['create'] = create_screen['id']
        else:
            payload['screens']['create'] = None
        if edit_screen:
            payload['screens']['edit'] = edit_screen['id']
        else:
            payload['screens']['edit'] = None
        if view_screen:
            payload['screens']['view'] = view_screen['id']
        else:
            payload['screens']['view'] = None
        response = requests.put(
            url=f"{self.baseurl}/rest/api/3/screenscheme/{scheme['id']}",
            auth=self.auth,
            data=json.dumps(payload),
            headers=headers,
        )
        if response.ok:
            return True
        else:
            raise Exception(f"Error in jira.client.update_screen_scheme\n{response.text}")
    def get_issue_links(self):
        response = requests.get(
            f"{self.baseurl}/rest/api/3/issueLinkType",
            auth=self.auth,
        )
        if response.ok:
           return response.json()['issueLinkTypes']
        else:
            raise Exception(f"Error in jira.client.get_issue_links\n{response.text}")
    def get_issue_link(self,name):
        links = self.get_issue_links()
        result = list(filter(lambda x: x['name'] == name, links))
        if len(result) == 1:
            return result[0]
        else:
            return {}
    def add_issue_link(self, name, inward, outward):
        if self.get_issue_link(name):
            print(f"Issue Link {name} already exist")
            return self.get_issue_link(name)
        else:
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            payload = json.dumps(
                {
                    "name": name,
                    "inward": inward,
                    "outward": outward
                }
            )
            response = requests.post(
                url=f"{self.baseurl}/rest/api/3/issueLinkType",
                auth=self.auth,
                data=payload,
                headers=headers,
            )
            if response.ok:
                return response.json()
            else:
                raise Exception(f"Error in jira.client.add_issue_link\n{response.text}")
            

    # User Management Related Methods Added & Enhanced by Jerry @ 02/08/2024
    # Create User: Experiemental --> required user_access_admin
    # API ref link: https://developer.atlassian.com/cloud/jira/platform/rest/v2/api-group-users/#api-rest-api-2-user-post
    def add_user(self, invite_user_email, product_access=[]):
        # check if a user already exist in the org
        user_exist = self.get_user(invite_user_email)
        if user_exist:
            raise Exception(f"Error in jira.client.add_user\n{invite_user_email} already exist")
        
        # In total 4 product access options: ["jira-core", "jira-servicedesk", "jira-product-discovery", "jira-software"]
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        payload = json.dumps(
            {
                "emailAddress":invite_user_email,
                "products": product_access # default as no product access if leave the list empty
            }
        )
        response = requests.post(
            url=f"{self.baseurl}/rest/api/3/user",
            auth=self.auth,
            headers=headers,
            data=payload
        )
        if response.ok:
           return response.json()
        else:
            raise Exception(f"Error in jira.client.add_user\n{response.text}")
        
    # Delete User: Experiemental --> user_access_admin
    # API ref link: https://developer.atlassian.com/cloud/jira/platform/rest/v2/api-group-users/#api-rest-api-2-user-delete
    def del_user(self, del_user_email):
        # check if a user already exist in the org
        user_exist = self.get_user(del_user_email)
        if user_exist:
            pass
        else:
            raise Exception(f"Error in jira.client.del_user\n{del_user_email} does not exist")
        
        # Try retriving accountId from user obj
        accountId = None
        try:
            accountId = user_exist['accountId']
            query = {'accountId': accountId}
        except Exception as e:
            raise Exception("Error in jira.client.del_user (accountId not provided)")
        
        # Build up an array of group memberships that site have for validate if the delete user is an admin
        import re
        pattern = r'https://([^.]+)\.atlassian'
        match = re.search(pattern, self.baseurl)
        sitename = None
        if match:
            sitename = match.group(1)
        admin_group_memberships = ['atlassian-addons-admin', 'system-administrators', 
                                  'org-admins', 'jira-admin', 'jira-user-access-admins', 
                                  'jira-servicemanagement-user-access-admins']
        admin_group_membership_formated = [admin_group_membership+'-'+sitename for admin_group_membership in admin_group_memberships]

        # Check if the user enrolled in admin level access, if yes -> return error
        user_enrolled_groups = self.get_user_groups(user_exist)
        user_enrolled_groups_names = [groupname['name'] for groupname in user_enrolled_groups]
        for user_enrolled_groups_name in user_enrolled_groups_names:
            if user_enrolled_groups_name in admin_group_membership_formated:
                raise Exception("Error in jira.client.del_user (Cannot automatically delete an admin level user)")
        
        # If the delete user is not an admin -> can remove
        response = requests.delete(
            url=f"{self.baseurl}/rest/api/3/user",
            params=query,
            auth=self.auth 
        )
        if response.ok:
            return f"User with accountId {accountId} is deleted"
        else:
            raise Exception(f"Error in jira.client.del_user\n{response.text}")
        
    def get_form_simplified_answer(self, issue, form):
        headers = {
            "Accept": "application/json"
        }
        response = requests.get(
            url=f"{self.forms_url}/issue/{issue.get_key()}/form/{form['id']}/format/answers",
            headers=headers,
            auth=self.auth 
        )
        if response.ok:
            return response.json()
        else:
            raise Exception(f"Error in jira.client.get_form_simplified_answer\n{response.text}")
        
    # Get User List: 
    # API ref: https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-users/#api-rest-api-3-users-search-get
    def get_all_users(self):
        get_all_users_url = f"{self.baseurl}/rest/api/3/users/search"
        headers = {
            "Accept": "application/json"
        }
        params = {
            'startAt': 0,
            'maxResults': 100000
        }
        response = requests.get(
            get_all_users_url,
            headers=headers,
            auth=self.auth
        )
        if response.ok:
            return response.json()
        else:
            raise Exception(f"Error in jira.client.get_all_users\n{response.text}")