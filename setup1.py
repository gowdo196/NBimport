from distutils.core import setup
import py2exe,os

setup(
options = { 
    "py2exe": 
    { 
        "includes": ["sys", "os", "ConfigParser", "time", "datetime",
                     "logging", "logging.handlers", "psycopg2", "psycopg2.extras", "msvcrt"],
        "dll_excludes": ["Secur32.dll", "SHFOLDER.dll"]
    }
},
console = ['DataImport.py']
)

