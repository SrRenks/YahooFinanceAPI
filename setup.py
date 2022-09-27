from distutils.core import setup
import os

# Optional project description in README.md:
current_directory = os.path.dirname(os.path.abspath(__file__))

try:
    with open(os.path.join(current_directory, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()
except Exception:
    long_description = ''
setup(

    # Project name:
    name='yfapi',

    # Project version number:
    version='1.0',

    # List a license for the project, eg. MIT License
    license='MIT',

    # Short description of your library:
    description='An unofficial library to capture data from Yahoo Finance in an easier way. Having access to capture data from various actions provided by the service.',

    # Long description of your library:
    long_description=long_description,

    long_description_content_type='text/markdown',

    # Your name:
    author='Ricardo Castro',

    # Your email address:
    author_email='srrenks@gmail.com',

    # Link to your github repository or website:
    url='https://github.com/SrRenks/yfinance',

    packages=['yfinance'],
    # List project dependencies:
    install_requires=['pandas', 'requests', 'tqdm', 'wheel'],

)
