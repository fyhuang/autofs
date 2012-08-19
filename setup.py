from setuptools import setup

setup(name='autofs',
      version='0.1',
      description='Automatically organizing and synchronizing filesystem',
      author='Yifeng Huang',
      author_email='me@nongraphical.com',
      url='https://github.com/fyhuang/autofs',

      packages=['autofs'],
      entry_points={
          'console_scripts': [
              'autofs = autofs:main',
              ],
          },

      install_requires=[
          'protobuf',
          ],
      )
