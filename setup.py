"""Build shim: all metadata lives in pyproject.toml, but a cythonized
extension can only be declared imperatively."""
import Cython.Build
from setuptools import Extension, setup

# The extension must live inside the package, otherwise the pure-python
# package shadows the top-level compiled module and it is never imported
ext = Extension(name="UltraDict2.UltraDict2", sources=["UltraDict2/UltraDict2.py"])

setup(
    cmdclass={"build_ext": Cython.Build.build_ext},
    ext_modules=Cython.Build.cythonize(
        ext, compiler_directives={"language_level": "3"}
    ),
)
