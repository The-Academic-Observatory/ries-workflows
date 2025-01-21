#!/bin/bash

# check dependencies using a local package manager if available
if   [[ ! -z `which brew` ]]; then brew install git unzip curl node   npm
elif [[ ! -z `which apt`  ]]; then apt  add     git unzip curl nodejs npm
elif [[ ! -z `which apk`  ]]; then apk  add     git unzip curl nodejs npm
fi

# abort if dependencies aren't found
if [[ -z `which unzip` ]]; then echo 'ERROR: `unzip` not found. Please install it with your package manager'; exit; fi
if [[ -z `which curl`  ]]; then echo 'ERROR: `curl`  not found. Please install it with your package manager'; exit; fi
if [[ -z `which node`  ]]; then echo 'ERROR: `node`  not found. Please install it with your package manager'; exit; fi
if [[ -z `which npm`   ]]; then echo 'ERROR: `npm`   not found. Please install it with your package manager'; exit; fi
if [[ -z `which gcloud`   ]]; then echo 'ERROR: `gcloud`   not found. Please install it with your package manager'; exit; fi

# install and audit package dependencies
npm install -g pnpm && pnpm install && pnpm audit

# suggest that a config be created
if [[ ! -f .config.json ]]; then
    cp config_example.json .config.json
    echo "Check your CONFIG settings at $PWD/.config.json"
fi
