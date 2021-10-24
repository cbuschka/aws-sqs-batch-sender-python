import os
import re
import setuptools
import subprocess

project_dir = os.path.abspath(os.path.dirname(__file__))

def get_readme():
    with open(os.path.join(project_dir, "readme.md"), "r", encoding='utf-8') as fh:
        readme = fh.read()
        return readme


def get_requirements():
    with open(os.path.join(project_dir, 'requirements.txt'), "r", encoding="utf-8") as f:
        requirements =[r.strip() for r in f.readlines()]
        return requirements


def get_version():
    proc = subprocess.Popen(['git', 'describe', '--dirty', '--tags'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = proc.communicate()
    result = re.search('^v([^\n]+)\n$', stdout.decode("utf-8"), re.S)
    if not result:
        raise ValueError("Invalid version: '{}'.".format(result))
    return result.group(1)


requirements = get_requirements()
version = get_version()
long_description = get_readme()

setuptools.setup(
    name="aws-sqs-batch-sender",
    version=version,
    install_requires=requirements,
    author="Cornelius Buschka",
    author_email="cbuschka@gmail.com",
    description="AWS SQS Batch Sender for boto3",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cbuschka/aws-sqs-batch-sender-python",
    packages=setuptools.find_packages(exclude=('tests','integration_tests')),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Software Development',
    ],
    python_requires='>=3.6',
)
