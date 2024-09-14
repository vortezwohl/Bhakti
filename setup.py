import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("LICENSE", "r", encoding="utf-8") as fh:
    license_content = fh.read()

setuptools.setup(
    name="bhakti",
    version='0.2.4',
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
    python_requires=">=3.6",
    install_requires=[
        'numpy>=1.26.4'
    ],
    entry_points={
        'console_scripts': [
            'bhakti = bhakti.boostrap.boostrap:__main__'
        ]
    },
    include_package_data=False
)
