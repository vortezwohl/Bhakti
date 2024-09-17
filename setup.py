import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("LICENSE", "r", encoding="utf-8") as fh:
    license_content = fh.read()

setuptools.setup(
    name="bhakti",
    version='0.2.18',
    author="vortezwohl",
    author_email="vortez.wohl@gmail.com",
    description="An easy-to-use vector database.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license=license_content,
    url="https://github.com/vortezwohl/Bhakti",
    project_urls={
        "Bug Tracker": "https://github.com/vortezwohl/Bhakti/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Natural Language :: English",
    ],
    package_dir={"": "."},
    packages=setuptools.find_packages(where="."),
    python_requires=">=3.10",
    install_requires=[
        'numpy>=2.1.1',
        'colorama>=0.4.6',
        'dipamkara>=0.3.8',
        'argparse>=1.4.0',
        'PyYAML>=6.0.2'
    ],
    entry_points={
        'console_scripts': [
            'bhakti = bhakti.bootstrap.bhakti_server:bhakti_entry_point'
        ]
    },
    include_package_data=False
)
