# python3 setup.py build_ext --inplace --force

from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np  # Added this line
from torch.utils.cpp_extension import BuildExtension, CppExtension
import subprocess

import torch


lib_path = torch.__file__[:-len('/__init__.py')]+'/lib'
print(lib_path)
inc_path = torch.__file__[:-len('/__init__.py')]+'/include'
_inc_path = torch.__file__[:-len('/__init__.py')]+'/include/torch/csrc/api/include'

extensions = [
    CppExtension(
        "my_cython_module",  # name of the output .so file
        ["src/my_cython_module.pyx"],  # source .pyx file
        # libraries=["m","c10","torch_cpu","torch"],  # C libraries to link against
        # language="c++",
        include_dirs=["src/headers"
                    #   ,inc_path,_inc_path
                      ],  # Added this line
        # requirements=["numpy"],  # Added this line
        # extra_compile_args=["-std=c++14"
        #                     ,f"-L{lib_path}"
        #                     ],  
        # include_dirs=["path/to/include/dirs"],  # if any include directories are needed
        # library_dirs=[lib_path],  # if any library directories are needed
    )
]
# LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/ohadr/.local/lib/python3.8/site-packages/torch/lib
setup(
    name="MyCythonModule",
    ext_modules=cythonize(extensions),
    cmdclass={'build_ext': BuildExtension}
)
