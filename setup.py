from setuptools import setup

setup(
    # Needed to silence warnings (and to be a worthwhile package)
    name='dgnutils',
    url='https://github.com/dnishiyama/run_commands/dgn_utils_module',
    author='Declan Nishiyama',
    author_email='dnishiyama@gmail.com',
    # Needed to actually package something
    packages=['dgnutils'],
    # Needed for dependencies
    install_requires=['pymysql'],
    # *strongly* suggested for sharing
    version='0.3.1',
    # The license can be anything you like
    license='MIT',
    description='package to maintain commonly used functions',
    # We will also need a readme eventually (there will be a warning)
    # long_description=open('README.txt').read(),
)
