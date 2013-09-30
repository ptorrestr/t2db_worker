from setuptools import setup

def readme():
    with open('README.md') as f:
        return f.read()

setup(
    name='t2db_worker',
    version='0.2.0',
    description='Worker for t2db',
    long_description = readme(),
    classifiers=[
      'Programming Language :: Python :: 3.2',
    ],
    url='http://github.com/ptorrest/t2db_worker',
    author='Pablo Torres',
    author_email='pablo.torres@deri.org',
    license='GNU',
    packages=['t2db_worker', 't2db_worker.tests'],
    install_requires=[
        't2db_objects >= 0.4.2',
	'twitter >= 1.10.0',
    ],
    entry_points = {
        'console_scripts':[
            't2db_worker = t2db_worker.worker:main'
        ]
    },
    test_suite='t2db_worker.tests',
    zip_safe = False
)
