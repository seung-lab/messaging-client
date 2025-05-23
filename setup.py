import os
import re
import codecs
import setuptools


def read(*parts):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, *parts), "r") as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="messagingclient",
    version=find_version("messagingclient", "__init__.py"),
    author="Akhilesh Halageri",
    description="Messaging Client",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/seung-lab/messaging-client",
    project_urls={
        "Bug Tracker": "https://github.com/seung-lab/messaging-client/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"messagingclient": "messagingclient"},
    packages=setuptools.find_packages(),
    install_requires=["numpy", "google-cloud-pubsub"],
    python_requires=">=3.6",
)
