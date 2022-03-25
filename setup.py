# Adapted from: https://github.com/pypa/sampleproject/blob/main/setup.py
from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / "README.md").read_text(encoding="utf-8")
__version__ = "0.1.0"

setup(
    name="magstar-client",
    version=__version__,
    description="A Python client library for interfacing with the CPI Magstar system",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    url="https://github.com/CPIProductionDataLab/magstar-python-client",
    author="Robert P. Cope (Computational Physics Inc.)",
    author_email="rcope@cpi.com",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Physics",
        "Topic :: Software Development :: Libraries",
        "Typing :: Typed"
    ],
    keywords="magstar, magnetometers, gmd, space weather",
    packages=find_packages(),  # Required
    python_requires=">=3.6, <4",
    install_requires=["requests>=2,<3", "dataclasses>=0.8"],
    project_urls={  # Optional
        "Computational Physics Inc.": "https://www.cpi.com/GMD.html"
    },
)
