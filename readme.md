Fusetools is a Python package with code to help you accomplish miscellaneous and business tasks.
With Fusetools you can access a wide variety of applications and APIs for different use cases.

Roadmap
---------------------

Fusetools currently has functionality for the following tools and applications, plus way more!

![alt text](https://github.com/fusecloud/fusetools/blob/master/docs/source/CC_ROADMAP.png)

Installation
--------------------

This package is in the [Python Package Index](http://pypi.python.org/pypi/FuseTools) so ``pip install FuseTools`` should
be enough.  You can also clone or fork it from here.

Minified Version
--------------------
Don't need all of FuseTools?  You can create your own variant with just the modules you want.

```python
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
```

Documentation 
---------

[Read the Docs](https://fusetools.readthedocs.io/)

Examples 
---------

* [cloud tools - AWS](https://github.com/fusecloud/fusetools/blob/master/examples/cloud_tools%20-%20AWS.ipynb)
* [cloud tools - Firebase](https://github.com/fusecloud/fusetools/blob/master/examples/cloud_tools%20-%20Firebase.ipynb)
* [comm tools](https://github.com/fusecloud/fusetools/blob/master/examples/comm_tools.ipynb)
* [db etl tools](https://github.com/fusecloud/fusetools/blob/master/examples/db_etl_tools.ipynb)
* [gsuite tools](https://github.com/fusecloud/fusetools/blob/master/examples/gsuite_tools.ipynb)


Licensing
---------

Fusetools is distributed under the MIT License.
