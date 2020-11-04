"""
Tools for automating package deployment tasks.

|pic1|
    .. |pic1| image:: ../images_source/pkg_tools/pypkg.jpeg
        :width: 25%

"""

import os
import pexpect
import sys
import os
from fusetools.transfer_tools import Local as TransferLocal
from fusetools.text_tools import Export
from distutils import dir_util, file_util


class Local:

    @classmethod
    def run_handle_cmds(cls):
        pass
        # todo: this lets you define commands, run them, and then handle outputs

    @classmethod
    def create_sub_pkg(cls, src_pkg_dir, src_pkg_name,
                       tgt_pkg_dir, tgt_pkg_name,
                       folder_list, file_list,
                       module_list=False,
                       install_pkg=False,
                       python_alias="python3"):

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
                           pkg_name="fusetools",
                           pkg_dir=False,
                           pkg_version="0.0.2",
                           os_type="unix"):
        """
        Compiles the Python package.

        :param pkg_name: Name for package.
        :param pkg_dir: Directory of setup.py file for package.
        :return: Command line logs for compilation steps.
        """
        if pkg_dir:
            os.chdir(pkg_dir)
            print(f'''pkg setup folder: {os.getcwd()}''')

        os.system("python setup.py sdist")
        if os_type != "unix":
            install_cmd = f'''pip install "{os.getcwd()}/dist/{pkg_name}-{pkg_version}.tar.gz"'''.replace("/", "\\")
        else:
            install_cmd = f'''pip install "{os.getcwd()}/dist/{pkg_name}-{pkg_version}.tar.gz"'''

        os.system(install_cmd)


class Terraform:

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
    # todo implement
    pass


class Docs:

    @classmethod
    def read_the_docs(cls):
        # https://readthedocs.org/projects/docs/downloads/pdf/latest/
        pass

    @classmethod
    def publish_pypi(cls, pkg_dir, api_key, python_name="python3"):
        """

        :param pkg_dir:
        :param api_key:
        :param python_name:
        :return:
        """
        # https://packaging.python.org/tutorials/packaging-projects/
        os.chdir(pkg_dir)
        input_name = "__token__"
        cmd = f"{python_name} -m twine upload --repository testpypi dist/*"
        child = pexpect.spawn(cmd, encoding='utf-8')
        child.logfile = sys.stdout
        child.expect(".*username:", timeout=None)
        child.sendline(input_name)
        child.expect(".*password:", timeout=None)
        child.sendline(api_key)
        child.expect(".*pypi")
