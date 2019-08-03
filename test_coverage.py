#!/usr/bin/env python

import coverage
import os, os.path

omit_directories = [
    "/usr/lib/*",
    os.path.join("notochord","test","*")
]

cov = coverage.Coverage(omit=omit_directories)
cov.start()

if os.path.exists("test.log"):
    os.remove("test.log")

from notochord.test import *
main()

cov.stop()
cov.save()
cov.html_report(directory="covhtml")
cov.report()
