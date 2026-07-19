from pathlib import Path

import Cython.Build
from setuptools import Extension, setup

# read the contents of your README file
this_directory = Path(__file__).parent
long_description = (this_directory / "readme.md").read_text()

# The extension must live inside the package, otherwise the pure-python
# package shadows the top-level compiled module and it is never imported
ext = Extension(name="UltraDict2.UltraDict2", sources=["UltraDict2/UltraDict2.py"])

setup(
    name="UltraDict2",
    description="Sychronized, streaming dictionary that uses shared memory as a backend",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Ronny Rentner",
    author_email="ultradict.code@ronny-rentner.de",
    url="https://github.com/mvanderlee/UltraDict2",
    cmdclass={"build_ext": Cython.Build.build_ext},
    packages=["UltraDict2"],
    zip_safe=False,
    ext_modules=Cython.Build.cythonize(
        ext, compiler_directives={"language_level": "3"}
    ),
    setup_requires=["cython>=0.24.1"],
    install_requires=["atomics2==1.1.0", "psutil"],
    python_requires=">=3.11",
)
