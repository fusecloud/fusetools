"""
Tools for automating package deployment tasks.

|pic1|
    .. |pic1| image:: ../images_source/pkg_tools/pypkg.jpeg
        :width: 25%

"""
import json
import nbsphinx
import requests
import os
import pexpect
import sys
import os
from fusetools.transfer_tools import Local as TransferLocal
from fusetools.text_tools import Export
from distutils import dir_util, file_util


class Local:
    """
    Functions for dealing with Local DevOps tasks.

    """

    @classmethod
    def run_handle_cmds(cls):
        pass
        # todo: this lets you define commands, run them, and then handle outputs

    @classmethod
    def create_sub_pkg(cls, src_pkg_dir, src_pkg_name,
                       tgt_pkg_dir, tgt_pkg_name,
                       folder_list, file_list,
                       module_list=False,
                       install_pkg=True,
                       os_type="unix",
                       pkg_version="0.0.1",
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
        for folder in folder_list:
            dir_util.copy_tree(src_pkg_dir + folder,
                               tgt_pkg_dir + folder)

        # copy files
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
version="{pkg_version}",
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

        # replace pkg references in files:
        Export.find_replace_text(
            directory=tgt_pkg_dir,
            find=src_pkg_name,
            replace=tgt_pkg_name,
            file_pattern="*.py")

        # compile
        os.chdir(tgt_pkg_dir)

        # compile & install package
        Local.compile_python_pkg(
            pkg_name=tgt_pkg_name,
            pkg_dir=tgt_pkg_dir,
            pkg_version=pkg_version,
            os_type=os_type,
            install_pkg=install_pkg
        )

    @classmethod
    def compile_python_pkg(cls,
                           pkg_dir,
                           pkg_name,
                           pkg_version="0.0.1",
                           os_type="unix",
                           install_pkg=True):
        """
        Compiles the Python package.

        :param pkg_name: Name for package.
        :param pkg_dir: Directory of setup.py file for package.
        :param install_pkg: Whether or not to install package.
        :return: Command line logs for compilation steps.

        """
        os.chdir(pkg_dir)
        print(f'''pkg setup folder: {os.getcwd()}''')
        os.system("python setup.py sdist")
        print(f'''Building requirements.txt''')
        # make requirements.txt
        os.system(f"pipreqs {pkg_name} --force")
        try:
            file_util.copy_file(pkg_dir + pkg_name + "/requirements.txt",
                                pkg_dir + "requirements.txt")
        except:
            pass

        if install_pkg:
            if os_type != "unix":
                install_cmd = f'''pip install "{os.getcwd()}/dist/{pkg_name}-{pkg_version}.tar.gz"'''.replace("/", "\\")
            else:
                install_cmd = f'''pip install "{os.getcwd()}/dist/{pkg_name}-{pkg_version}.tar.gz"'''

            os.system(install_cmd)
            os.system("pip install -r requirements.txt")


class ReadTheDocs:
    """
    Functions for dealing with software documentation

    """

    @classmethod
    def get_projects(cls, token, project_name=False):
        URL = f'https://readthedocs.org/api/v3/projects/{project_name if project_name else ""}'
        TOKEN = token
        HEADERS = {'Authorization': f'token {TOKEN}'}
        response = requests.get(URL, headers=HEADERS)
        return response

    @classmethod
    def create_project(cls, token, project_def_path):
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
    Functions for building Sphinx documentation

    """

    @classmethod
    def build_sphinx_docs(cls, os_type,
                          pkg_name,
                          pkg_dir,
                          python_alias="python",
                          docs_folder_name="docs",
                          compile_pkg=False,
                          show_html=True):
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

    """

    @classmethod
    def publish_pypi(cls, pkg_dir, api_key, test_prod_env="test", python_alias="python3"):
        """
        Publishes a Python package to the PyPi repository

        :param pkg_dir: Directory containing package to publish
        :param api_key: PyPi API key
        :param python_name: Locally installed python name (ex: python -m ...)
        :return: Published Python package on PyPi

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


class Terraform:
    """
    Functions for interacting with Terraform

    """

    @classmethod
    def create_backend_file(cls, tf_folder, init=False):
        # todo; create and save backend.tf
        if init:
            os.system("terraform init")
        pass

    # @classmethod
    # def create_variable_file(cls, variable_list):
    #     # todo; create and save variables.tf
    #     for idx, variable in enumerate(variable_list):
    #         variable_ = f'''
    #             variable "profile" {
    #                 type="string"
    #         default = "default"
    #         }
    #             '''
    #         pass


class GitHub:
    """
    Functions for interacting with GitHub

    """

    @classmethod
    def commit_push(cls, repo_dir,
                    commit_msg,
                    tgt_branch,
                    user, pwd):
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
        child.expect(f".*{tgt_branch}")
