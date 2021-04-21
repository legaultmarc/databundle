from setuptools import setup, find_packages


def setup_package():
    setup(
        name="databundle",
        version="0.1",
        description="A package to aggregate tabular data across various "
                    "sources",
        url="https://github.com/legaultmarc/databundle",
        license="MIT",
        zip_safe=False,
        entry_points={
            "console_scripts": [
                "databundle=databundle.scripts.bundler_script:main",
            ],
        },
        install_requires=["numpy >= 1.12.0", "pandas >= 0.19.0",
                          "pyyaml >= 3.12"],
        packages=find_packages(),
        classifiers=["Development Status :: 4 - Beta",
                     "Intended Audience :: Science/Research",
                     "Operating System :: Unix",
                     "Operating System :: POSIX :: Linux",
                     "Operating System :: MacOS :: MacOS X",
                     "Operating System :: Microsoft",
                     "Programming Language :: Python",
                     "Programming Language :: Python :: 3.4",
                     "Programming Language :: Python :: 3.5",
                     "Programming Language :: Python :: 3.6"],
        keywords="data management",
    )


if __name__ == "__main__":
    setup_package()
