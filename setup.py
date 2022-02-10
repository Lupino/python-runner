try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

packages = ['runner']

requires = []

setup(
    name='runner',
    version='0.1.0',
    description='',
    author='Li Meng Jun',
    author_email='lmjubuntu@gmail.com',
    url='https://github.com/Lupino/python-runner',
    packages=packages,
    package_dir={'runner': 'runner'},
    include_package_data=True,
    install_requires=requires,
)
