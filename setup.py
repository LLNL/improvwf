from setuptools import setup, find_packages

setup(name='improvwf',
      description='A tool for specifying and conducting dynamic '
                  'workflows in concert with maestrowf.',
      version='1.0',
      author='Thomas Desautels',
      author_email='desautels2@llnl.gov',
      url='',
      license='NOT_FOR_EXTERNAL_USE',
      packages=find_packages(),
      entry_points={
        'console_scripts': [
            'improv = improvwf.improv:main'
            ]
      },
      install_requires=[
        'numpy',
        'mysql',
        'pandas',
        'scikit-learn',
        'scipy',
        'PyYaml',
        'filelock',
        'tabulate',
        'maestrowf==1.1.4',
        'llnl-sina[mysql]>=1.8.0'
        ],
      extras_require={
        'test': ['pytest']},
      classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        ]
      )
