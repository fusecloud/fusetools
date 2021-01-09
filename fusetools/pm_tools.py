"""
Functions for interacting with Project Management Tools.

|pic1| |pic2|
    .. |pic1| image:: ../images_source/pm_tools/asana2.png
        :width: 25%
    .. |pic2| image:: ../images_source/pm_tools/workfront2.png
        :width: 40%
"""

import json
import os
import time
import os
import time
import pandas as pd
import requests


class Workfront:
    """
    Functions for interacting with Workfront.

    .. image:: ../images_source/pm_tools/workfront2.png
    """

    @classmethod
    def wf_login(cls, wf_name, wf_pwd, proxies=None):
        """
        Authenticates a user for a given Workfront login/password.

        :param wf_name: Workfront username.
        :param wf_pwd: Workfront password.
        :param proxies: Proxies to include on request (Optional).
        :return: SessionId for authenticated user.
        """

        url = "https://xxxx.my.workfront.com/attask/api/v9.0/login?username=" + \
              wf_name + \
              "&password=" + \
              wf_pwd

        try:
            r = requests.post(url, proxies=proxies)
        except:
            time.sleep(3)
            r = requests.post(url, proxies=proxies)

        # get session details
        sessiondetails = pd.read_json(r.content)
        # get sessionID
        sessionid = sessiondetails[
            sessiondetails.index == "sessionID"
            ]['data'][0]

        return sessionid

    @classmethod
    def wf_pull_projects_status(cls, status, wf_api_key, proxies=None):
        """
        Retrieves Workfront projects in a given project status.

        :param status: Project status to search for projects.
        :param wf_api_key: Workfront API key.
        :param proxies: Proxies to include on request (Optional).
        :return: JSON response for API call.
        """
        url = "https://xxx.my.workfront.com" + \
              '/attask/api/v9.0/project/search' + \
              "?status=" + status + \
              "&" + \
              "$$LIMIT=2000"

        r = requests.get(url, proxies=proxies,
                         params={"apiKey": wf_api_key})

        return r

    @classmethod
    def wf_pull_projects_custom_field(cls, wf_api_key, proxies=None):
        """
        Retrieves Workfront projects with a given custom field.

        :param wf_api_key: Workfront API key.
        :param proxies: Proxies to include on request (Optional).
        :return: JSON response for API call.
        """
        url = "https://xxx.my.workfront.com" + \
              '/attask/api/v9.0/project/search?' + \
              "DE:XXXX=YYYY" + \
              "&" + \
              "$$LIMIT=2000" + \
              "&" + \
              "fields=['*','DE:XXXX']"

        r = requests.get(url, proxies=proxies,
                         params={"apiKey": wf_api_key})

        return r

    @classmethod
    def wf_pull_issues_custom_field(cls, wf_api_key, proxies=None):
        """
        Retrieves Workfront issues with a given custom field.

        :param proxies: Proxies to include on request (Optional).
        :param wf_api_key: Workfront API key.
        :return: JSON response for API call.
        """
        url = "https://xxx.my.workfront.com" + \
              '/attask/api/v9.0/issue/search?' + \
              "DE:XXXX=YYYY" + \
              "&" + \
              "$$LIMIT=2000" + \
              "&" + \
              "fields=['*','DE:XXXX']"

        r = requests.get(url, proxies=proxies,
                         params={"apiKey": wf_api_key})

        return r

    @classmethod
    def wf_pull_tasks_name(cls, name_string, wf_api_key, proxies=None, assignee=False):
        """
        Retrieves Workfront tasks with a name string.

        :param name_string: String to search tasks for.
        :param wf_api_key: Workfront API Key.
        :param proxies: Proxies to include on request (Optional).
        :param assignee: Additional filter on task assignee name.
        :return: JSON response for API call.
        """
        if assignee:
            url = "https://xxx.my.workfront.com" + \
                  "/attask/api/v9.0/task/search?name=" + name_string + "&name_Mod=contains" + \
                  "&" + \
                  "$$LIMIT=2000"

            url = url + "&assignedTo:firstName=" + assignee

        else:
            url = "https://xxx.my.workfront.com" + \
                  "/attask/api/v9.0/task/search?name=" + name_string + "&name_Mod=contains" + \
                  "&" + \
                  "$$LIMIT=2000"

        r = requests.get(url, proxies=proxies, params={'apiKey': wf_api_key})

        return r

    @classmethod
    def wf_pull_tasks_status(cls, status, wf_api_key, proxies=None, assignee=False):
        """
        Retrieves Workfront tasks with a given status.

        :param status: Workfront status category.
        :param proxies: Proxies to include on request (Optional).
        :param wf_api_key: Workfront API Key.
        :param assignee: Additional filter on task assignee name.
        :return: JSON response for API call.
        """
        if assignee:
            url = "https://xxx.my.workfront.com" + \
                  "/attask/api/v9.0/task/search?status=" + status + \
                  "&" + \
                  "$$LIMIT=2000"

            url = url + "&assignedTo:firstName=" + assignee

        else:
            url = "https://xxx.my.workfront.com" + \
                  "/attask/api/v9.0/task/search?status=" + status + \
                  "&" + \
                  "$$LIMIT=2000"

        r = requests.get(url, proxies=proxies, params={'apiKey': wf_api_key})

        return r

    @classmethod
    def wf_upload_file(cls, wf_api_key, file, file_path, obj_id, obj_type, proxies=None):
        """
        Uploads a file to Workfront.

        :param wf_api_key: Workfront API Key
        :param file: Name of file to upload.
        :param file_path: Name of filepath for file to upload.
        :param obj_id: Id of Workfront object to upload file to.
        :param obj_type: Type of object to upload on Workfront.
        :param proxies: Proxies to include on request (Optional).
        :return: JSON response for API call.
        """
        url = "https://xxx.my.workfront.com/attask/api/v9.0/upload"

        multipart_form_data = {
            'uploadedFile': (file, open(file_path + file, 'rb')),
            'action': ('', 'store'),
            'path': ('', file_path)
        }

        response = requests.post(url,
                                 proxies=proxies,
                                 params={"apiKey": wf_api_key},
                                 files=multipart_form_data)

        handle = json.loads(response.content)['data']['handle']
        url = 'https://xxx.my.workfront.com/attask/api/v9.0/document?'
        url = url.replace('\n', '')
        r = requests.post(url,
                          proxies=proxies,
                          params={"apiKey": wf_api_key,
                                  'name': file,
                                  'handle': handle,
                                  'docObjCode': obj_type,
                                  'objID': obj_id,
                                  })

        return r


class Asana:
    """
    Functions for interacting with Asana.

    .. image:: ../images_source/pm_tools/asana2.png

    """

    @classmethod
    def pull_tasks_for_project(cls, asana_token, project):
        """
        Retrieve tasks on an Asana project.

        :param asana_token: Asana API token.
        :param project: Asana project Id.
        :return: Project tasks.
        """
        bearerToken = 'Bearer ' + asana_token
        header = {'Authorization': bearerToken}

        url = "https://app.asana.com/api/1.0/projects/" + project + "/tasks?opt_fields=name"
        try:
            r = requests.get(url, headers=header)
        except:
            time.sleep(3)
            r = requests.get(url, headers=header)
            pass

        return r

    @classmethod
    def get_task_detail(cls, asana_token, task_id):
        """
        Retrieve Asana task details.

        :param asana_token: Asana API token.
        :param task_id: Asana task Id.
        :return: Project task details.
        """
        bearerToken = 'Bearer ' + asana_token
        header = {'Authorization': bearerToken}
        url = "https://app.asana.com/api/1.0/tasks/" + str(task_id)
        r = requests.get(url, headers=header)
        return r

    @classmethod
    def get_project_tasks(cls, asana_token, project):
        """
        Retrieve tasks on an Asana project.

        :param asana_token: Asana API token.
        :param project: Asana project Id.
        :return: Project tasks.
        """
        bearerToken = 'Bearer ' + asana_token
        header = {'Authorization': bearerToken}
        options = {
            "data": {
                "projects": [project]
            }}

        url = 'https://app.asana.com/api/1.0/projects/' + project + '/tasks'

        try:
            r = requests.get(url, headers=header, json=options)
        except:
            r = requests.get(url, headers=header, json=options)
            pass

        return r

    @classmethod
    def create_task(cls, asana_token, project, taskName, taskDue, assignee, taskNotes=False):
        """
        Creates an Asana task.

        :param asana_token: Asana API token.
        :param project: Asana project id.
        :param taskName: Name of task.
        :param taskDue: Due date for task.
        :param assignee: Assigned person for task.
        :param taskNotes: Notes on task.
        :return: API call response.
        """
        '''
        creates a task in asana via API
        '''
        bearerToken = 'Bearer ' + asana_token
        header = {'Authorization': bearerToken}

        options = {
            "data": {
                "projects": [project],
                "name": taskName,
                "notes": taskNotes if taskNotes else "",
                "assignee": assignee if assignee else "",
                "due_on": taskDue
            }
        }

        url = 'https://app.asana.com/api/1.0/tasks'
        try:
            r = requests.post(url, headers=header, json=options)
        except:
            r = requests.post(url, headers=header, json=options)
            pass

        return r

    @classmethod
    def move_task(cls):
        pass
