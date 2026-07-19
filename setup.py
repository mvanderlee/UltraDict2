from pathlib import Path

import Cython.Build
from setuptools import Extension, setup
from setuptools.command.build_py import build_py as _build_py

# read the contents of your README file
this_directory = Path(__file__).parent
long_description = (this_directory / "readme.md").read_text()

# The extension must live inside the package, otherwise the pure-python
# package shadows the top-level compiled module and it is never imported
ext = Extension(name="UltraDict.UltraDict", sources=["UltraDict.py"])


class build_py(_build_py):
    def find_package_modules(self, package, package_dir):
        # setup.py lives in the package dir but must not ship in the wheel
        return [m for m in super().find_package_modules(package, package_dir) if m[1] != 'setup']


setup(
    name='UltraDict2',
    description='Sychronized, streaming dictionary that uses shared memory as a backend',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Ronny Rentner',
    author_email='ultradict.code@ronny-rentner.de',
    url='https://github.com/ronny-rentner/UltraDict',
    cmdclass={'build_ext': Cython.Build.build_ext, 'build_py': build_py},
    package_dir={'UltraDict': '.'},
    packages=['UltraDict'],
    zip_safe=False,
    ext_modules=Cython.Build.cythonize(ext, compiler_directives={'language_level' : "3"}),
    setup_requires=['cython>=0.24.1'],
    install_requires=['atomics2==1.1.0', 'psutil'],
    python_requires=">=3.11",
)
