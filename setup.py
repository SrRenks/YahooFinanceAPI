from setuptools import setup
import os


current_directory = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(current_directory, 'README.md'), encoding='utf-8') as f:
    README = f.read()

setup(
    name='yfapi',
    version='1.5.3',
    license='MIT',
    description='An unofficial library to capture data from Yahoo Finance API in an easier way. Having access to capture data from various actions provided by the service.',
    long_description_content_type="text/markdown",
    long_description=README,
    author='Ricardo Castro',
    author_email='srrenks@gmail.com',
    url='https://github.com/SrRenks/YahoofinanceAPI',
    packages=['yfapi'],
    install_requires=['pandas', 'requests', 'tqdm', 'openpyxl'],
)
