Introduction
=======================================

FuseTools is a Python package to help you accomplish miscellaneous and business tasks.
With FuseTools you can access a wide variety of applications and APIs for different use cases.

Roadmap
---------------------

FuseTools has functionality to interact with many applications and services including the following:

.. image:: CC_ROADMAP.png

Download and install
--------------------

This package is in the `Python Package Index
<http://pypi.python.org/pypi/fusetools>`__, so ``pip install fusetools`` should be enough.  You can also clone it on `Github
<http://github.com/fusecloud/fusetools>`__.

Minified Version
--------------------
Don't need all of FuseTools?  You can create your own variant with just the modules you want.

.. code-block:: python

    from fusetools.devops_tools import Local

    Local.create_sub_pkg(
        src_pkg_dir=src_pkg_dir, #source folder of existing package
        src_pkg_name="fusetools", #existing package name
        tgt_pkg_dir=sav_dir, #new package folder
        tgt_pkg_name=tgt_pkg_name, #new package name
        folder_list=False, #folders to copy over
        file_list=False, #files to copy over
        # specify the modules you want to use
        module_list=["gsuite_tools.py",
                     "financial_tools.py"],
        install_pkg=False, #whether or not to install new package
        python_alias="python" #your OS's python alias
    )

    # create init.py file
    myBat = open(sav_dir + f"{tgt_pkg_name}/__init__.py", 'w+')
    myBat.write(str(""))
    myBat.close()


Licensing
---------

Fusetools is distributed under the MIT License.

.. toctree::
   :maxdepth: 1
   :caption: Contents

   introduction
   examples
   changes
   links
   fusetools

.. autosummary::
   :toctree: _autosummary
   :template: custom-module-template.rst
   :recursive:





