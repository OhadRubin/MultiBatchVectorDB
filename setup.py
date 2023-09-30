# python3 setup.py build_ext --inplace --force

from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np  # Added this line
from torch.utils.cpp_extension import BuildExtension, CppExtension
import subprocess


extensions = [
    CppExtension(
        "my_cython_module",  # name of the output .so file
        ["src/my_cython_module.pyx"],  # source .pyx file
        include_dirs=["src/headers"
                      ],  # Added this line
    )
]
# LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/ohadr/.local/lib/python3.8/site-packages/torch/lib
setup(
    name="MyCythonModule",
    ext_modules=cythonize(extensions),
    cmdclass={'build_ext': BuildExtension}
)
