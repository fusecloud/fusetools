"""
Tools for automating package deployment tasks.

|pic1| |pic2| |pic3| |pic4| |pic5|
    .. |pic1| image:: ../images_source/devops_tools/local_folder.png
        :width: 20%
    .. |pic2| image:: ../images_source/devops_tools/rtd.png
        :width: 20%
    .. |pic3| image:: ../images_source/devops_tools/sphinx.png
        :width: 20%
    .. |pic4| image:: ../images_source/devops_tools/pypi.jpeg
        :width: 20%
    .. |pic5| image:: ../images_source/devops_tools/github.png
        :width: 20%

"""
import json
import requests
import pandas as pd
import os
import pexpect
import sys
import os
from fusetools.transfer_tools import Local as TransferLocal
from fusetools.text_tools import Export
from distutils import dir_util, file_util
import nbsphinx


class Local:
    """
    Functions for dealing with Local DevOps tasks.

    .. image:: ../images_source/devops_tools/local_folder.png
    """

    @classmethod
    def create_sub_pkg(cls, src_pkg_dir, src_pkg_name,
                       tgt_pkg_dir, tgt_pkg_name,
                       folder_list, file_list,
                       module_list=False,
                       install_pkg=False,
                       python_alias="python3"):
        """
        Creates a package from another package using specified details.

        :param src_pkg_dir: Directory of source package
        :param src_pkg_name: Name of source package
        :param tgt_pkg_dir: Directory of target package
        :param tgt_pkg_name: Name of target package
        :param folder_list: List of file folders to copy from source package directory
        :param file_list: List of files to copy from source package directory
        :param module_list: List of package modules to copy from source package
        :param install_pkg: Whether or not to install the package after copying
        :param python_alias: System alias for Python to compile package (ex: python/python3 setup.py sdist)
        :return: Copied package directory

        """

        if not module_list:
            module_list = [x for x in \
                           os.listdir(src_pkg_dir + "/fusetools") \
                           if ".py" in x]

        try:
            os.mkdir(tgt_pkg_dir)
        except:
            # delete files in the folder
            TransferLocal.clear_delete_directory(
                directory=tgt_pkg_dir,
                method="clear")

        # copy folders and contents
        if folder_list:
            for folder in folder_list:
                dir_util.copy_tree(src_pkg_dir + folder,
                                   tgt_pkg_dir + folder)

        # copy files
        if file_list:
            for file in file_list:
                file_util.copy_file(src_pkg_dir + file,
                                    tgt_pkg_dir + file)

        # make pkg folder
        os.mkdir(tgt_pkg_dir + tgt_pkg_name)

        # copy modules
        for module in module_list:
            file_util.copy_file(
                src_pkg_dir + src_pkg_name + "/" + module,
                tgt_pkg_dir + tgt_pkg_name + "/" + module
            )

        # make setup.py
        setup_py_text = \
            f'''
from setuptools import setup, find_packages\n
setup(\n
name="{tgt_pkg_name}",
version="0.0.1",
packages=["{tgt_pkg_name}"]
)
'''.strip()

        myBat = open(tgt_pkg_dir + "setup.py", 'w+')
        (
            myBat
                .write(
                str(setup_py_text)
            )
        )
        myBat.close()

        # make requirements.txt
        os.system(f"pipreqs {tgt_pkg_dir}")

        # replace pkg references in files:
        Export.find_replace_text(
            directory=tgt_pkg_dir,
            find=src_pkg_name,
            replace=tgt_pkg_name,
            file_pattern="*.py")

        # compile
        os.chdir(tgt_pkg_dir)
        os.system(f"{python_alias} setup.py sdist")

        # install package
        if install_pkg:
            os.chdir(tgt_pkg_dir + "dist")
            os.getcwd()
            os.system(f"pip install {os.listdir(os.getcwd())[0]}")

    @classmethod
    def compile_python_pkg(cls,
                           pkg_dir,
                           pkg_name,
                           pkg_version="0.0.1",
                           os_type="unix",
                           add_docs=False,
                           install_pkg=True):
        """
        Compiles the Python package.

        :param pkg_name: Name for package.
        :param pkg_dir: Directory of setup.py file for package.
        :param install_pkg: Whether or not to install package.
        :return: Command line logs for compilation steps.

        """
        os.chdir(pkg_dir)
        # attempt to clear our dist folder
        try:
            [os.remove(pkg_dir + "dist/" + x) for x in os.listdir(pkg_dir + "dist")]
        except:
            pass

        # grep setup.py and find version number
        with open(pkg_dir + "setup.py", "r") as fp:
            setup_text = fp.readlines()

        prior_pkg_version = (
            str(setup_text)
                .split("version=")[1]
                .split(",")[0]
                .replace('"', "")
        )

        # see if the version number in setup.py
        # is different than the one we've specified, replace if so
        if prior_pkg_version != pkg_version:
            Export.find_replace_text(
                directory=pkg_dir,
                find=prior_pkg_version,
                replace=pkg_version,
                file_pattern="setup.py"
            )

        os.system("python setup.py sdist")
        print(f'''Building requirements.txt''')
        # make sphinx_requirements.txt
        os.system(f"pipreqs {pkg_name} --force")

        # concatenate requirements files
        if add_docs:
            # main directory
            Export.concat_text_files(
                input_files=[
                    pkg_dir + pkg_name + "/requirements.txt",
                    pkg_dir + "docs/" + "sphinx_requirements.txt"
                ],
                output_file=pkg_dir + "requirements.txt"
            )

            # docs directory
            Export.concat_text_files(
                input_files=[
                    pkg_dir + pkg_name + "/requirements.txt",
                    pkg_dir + "docs/" + "sphinx_requirements.txt"
                ],
                output_file=pkg_dir + "docs/" + "requirements.txt"
            )
        else:
            try:
                os.remove(pkg_dir + "requirements.txt")
            except:
                pass
            file_util.move_file(
                src=pkg_dir + pkg_name + "/requirements.txt",
                dst=pkg_dir + "requirements.txt"
            )

        if install_pkg:
            if os_type != "unix":
                install_cmd = f'''pip install "{os.getcwd()}/dist/{pkg_name}-{pkg_version}.tar.gz"'''.replace("/", "\\")
            else:
                install_cmd = f'''pip install "{os.getcwd()}/dist/{pkg_name}-{pkg_version}.tar.gz"'''

            os.system(install_cmd)
            os.system("pip install -r requirements.txt")


class ReadTheDocs:
    """
    Functions for dealing with software documentation.

    .. image:: ../images_source/devops_tools/rtd.png
    """

    @classmethod
    def get_projects(cls, token, project_name=False):
        """
        Get projects on RTD, pulls a specific project if specified.

        :param token: RTD API token.
        :param project_name: Project name (optional).
        :return: JSON response from API call.
        """
        URL = f'https://readthedocs.org/api/v3/projects/{project_name if project_name else ""}'
        TOKEN = token
        HEADERS = {'Authorization': f'token {TOKEN}'}
        response = requests.get(URL, headers=HEADERS)
        return response

    @classmethod
    def create_project(cls, token, project_def_path):
        """
        Creates a project on RTD.

        :param token: RTD API token.
        :param project_def_path: Local path to project files.
        :return: JSON response from API call.
        """
        URL = 'https://readthedocs.org/api/v3/projects/'
        TOKEN = token
        HEADERS = {'Authorization': f'token {TOKEN}'}
        data = json.load(open(project_def_path, 'rb'))
        response = requests.post(
            URL,
            json=data,
            headers=HEADERS,
        )
        return response

    @classmethod
    def update_project(cls, token, project_def_path, project_name):
        """
        Updates a project on RTD.

        :param token: RTD API token.
        :param project_def_path: Local path to project files.
        :param project_name: Name of project on RTD.
        :return: JSON response from API call.
        """
        URL = f'https://readthedocs.org/api/v3/projects/{project_name}/'
        TOKEN = token
        HEADERS = {'Authorization': f'token {TOKEN}'}
        data = json.load(open(project_def_path, 'rb'))
        response = requests.patch(
            URL,
            json=data,
            headers=HEADERS,
        )
        return response


class Sphinx:
    """
    Functions for building Sphinx documentation.

    .. image:: ../images_source/devops_tools/sphinx.png
    """

    @classmethod
    def build_sphinx_docs(cls, os_type,
                          pkg_name,
                          pkg_dir,
                          python_alias="python",
                          docs_folder_name="docs",
                          compile_pkg=False,
                          show_html=True):
        """
        Creates Sphinx documentation for a Python package.

        :param os_type: Type of operating system the program is running on.
        :param pkg_name: Name of Python package if being compiled.
        :param pkg_dir: Directory containing Python package.
        :param python_alias: Name of Python alias on machine (ex: python3, python)
        :param docs_folder_name: Folder name containing documents in Python package directory.
        :param compile_pkg: Whether or not to compile the Python package before creating documentation.
        :param show_html: Whether or not to show the Sphinx documentation in the browser after build.
        :return:
        """
        if compile_pkg:
            Local.compile_python_pkg(
                pkg_name=pkg_name,
                pkg_dir=pkg_dir,
                os_type=os_type,
                install_pkg=True
            )

        os.chdir(pkg_dir + docs_folder_name)
        print(f"nbsphinx version: {nbsphinx.__version__}")
        os.system("make html")
        if show_html:
            os.system(f"{python_alias} -m http.server")


class PyPi:
    """
    Functions for distributing software packages

    .. image:: ../images_source/devops_tools/pypi.jpeg
    """

    @classmethod
    def get_pkg_dtl(cls, pkg_name):
        """
        Get the details for a Python package on PyPi.

        :param pkg_name: Name of Python package to search for on PyPi.
        :return: JSON API response call.
        """
        ret = requests.get(f"https://pypi.python.org/pypi/{pkg_name}/json")
        return json.loads(ret.content)

    @classmethod
    def publish_pypi(cls, pkg_dir, api_key, test_prod_env="test", python_alias="python3"):
        """
        Publishes a Python package to the PyPi repository.

        :param pkg_dir: Directory containing package to publish.
        :param api_key: PyPi API key.
        :param test_prod_env: PyPi environment ('test' or other string (production)).
        :param python_alias: Locally installed python name (ex: python -m ...).
        :return: Published Python package on PyPi.

        """
        # https://packaging.python.org/tutorials/packaging-projects/
        os.chdir(pkg_dir)
        input_name = "__token__"
        if test_prod_env == "test":
            cmd = f"{python_alias} -m twine upload --repository testpypi dist/* --verbose"
        else:
            cmd = f"{python_alias} -m twine upload --repository pypi dist/*  --verbose"

        child = pexpect.spawn(cmd, encoding='utf-8')
        child.logfile = sys.stdout
        child.expect(".*username:", timeout=None)
        child.sendline(input_name)
        child.expect(".*password:", timeout=None)
        child.sendline(api_key)
        child.expect(".*pypi")


class GitHub:
    """
    Functions for interacting with GitHub.

    .. image:: ../images_source/devops_tools/github.png
    """

    @classmethod
    def commit_push(cls, repo_dir,
                    commit_msg,
                    tgt_branch,
                    user, pwd):
        """
        Performs a commit and push for a Git repository.

        :param repo_dir: Directory of repository.
        :param commit_msg: Commit message.
        :param tgt_branch: Name of branch to target.
        :param user: GitHub username.
        :param pwd: GitHub password.
        :return: Command line responses from actions performed.
        """
        os.chdir(repo_dir)
        os.getcwd()

        os.system("git add --all")
        os.system(f"git commit -m '{commit_msg}'")
        child = pexpect.spawn(f"git push origin {tgt_branch}", encoding='utf-8')
        child.logfile = sys.stdout
        child.expect(".*Username", timeout=None)
        child.sendline(user)
        child.expect(".*Password", timeout=None)
        child.sendline(pwd)
        try:
            child.expect(f".*{tgt_branch}")
        except:
            child.expect(f".*Everything")

    @classmethod
    def get_repo_commits(cls, owner, repo, gh_token):
        """

        :param owner:
        :param repo:
        :param gh_token:
        :return:
        """

        url = f"https://api.github.com/repos/{owner}/{repo}/commits"

        headers = {'Authorization': f'''token {gh_token}'''}
        r = requests.get(url, headers=headers)

        encoding = 'utf-8'
        r_json = json.loads(r.content.decode(encoding))
        r_list = [x.get("commit") for x in r_json]

        df = pd.DataFrame.from_dict(r_list).reset_index(drop=True)
        df['sha'] = df.apply(lambda x: x['url'].split("commits/")[-1], axis=1)

        return df

    @classmethod
    def get_commit_dtl(cls, owner, repo, commit_ref):
        """

        :param owner:
        :param repo:
        :param commit_ref:
        :return:
        """
        headers = {'Authorization': f'''token {os.environ['gh_cli_code']}'''}
        encoding = 'utf-8'
        url = f"https://api.github.com/repos/{owner}/{repo}/commits/{commit_ref}"
        r = requests.get(url, headers=headers)
        r_json = json.loads(r.content.decode(encoding))

        return r_json
