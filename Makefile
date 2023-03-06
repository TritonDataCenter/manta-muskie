#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2021 Joyent, Inc.
#

#
# Makefile: Muskie, the public-facing Manta API server
#

#
# Files
#
DOC_FILES =		index.md
RESTDOWN_FLAGS =	--brand-dir=docs/bluejoy
RESTDOWN_EXT =		.md

JS_FILES :=		bin/mlocate bin/mpicker \
			$(shell ls *.js) \
			$(shell find lib test -name '*.js')
JSL_CONF_NODE =		tools/jsl.node.conf
JSL_FILES_NODE =	$(JS_FILES)
JSSTYLE_FILES =		$(JS_FILES)
JSSTYLE_FLAGS =		-f tools/jsstyle.conf

SMF_MANIFESTS_IN =	smf/manifests/muskie.xml.in \
			smf/manifests/haproxy.xml.in

#
# Variables
#
NAME 			= muskie
NODE_PREBUILT_TAG       = zone64
NODE_PREBUILT_VERSION	:= v6.17.1
#  minimal-64-lts 19.4.0
NODE_PREBUILT_IMAGE     = 5417ab20-3156-11ea-8b19-2b66f5e7a439

TEST_JOBS ?= 10
TEST_TIMEOUT_S ?= 1200
TEST_FILTER ?= .*

TAP_EXEC := ./node_modules/.bin/tap

ENGBLD_USE_BUILDIMAGE	= true
ENGBLD_REQUIRE		:= $(shell git submodule update --init deps/eng)
include ./deps/eng/tools/mk/Makefile.defs
TOP ?= $(error Unable to access eng.git submodule Makefiles.)

include ./deps/eng/tools/mk/Makefile.node_modules.defs
ifeq ($(shell uname -s),SunOS)
	include ./deps/eng/tools/mk/Makefile.node_prebuilt.defs
	include ./deps/eng/tools/mk/Makefile.agent_prebuilt.defs
	include ./deps/eng/tools/mk/Makefile.smf.defs
else
	NPM=npm
	NODE=node
	NPM_EXEC=$(shell which npm)
	NODE_EXEC=$(shell which node)
endif

RELEASE_TARBALL :=	$(NAME)-pkg-$(STAMP).tar.gz
ROOT :=			$(shell pwd)
RELSTAGEDIR :=		/tmp/$(NAME)-$(STAMP)

# This image is triton-origin-x86_64-19.4.0
BASE_IMAGE_UUID = 59ba2e5e-976f-4e09-8aac-a4a7ef0395f5
BUILDIMAGE_NAME = mantav2-webapi
BUILDIMAGE_DESC	= Manta webapi
BUILDIMAGE_PKGSRC = haproxy-2.0.25
AGENTS		= amon config registrar

ifeq ($(shell uname -s),SunOS)
	export PATH:=$(TOP)/build/agent-python:$(PATH)
endif

#
# Repo-specific targets
#

.PHONY: all
all: $(SMF_MANIFESTS) $(STAMP_NODE_MODULES) manta-scripts

check:: python2-symlink

.PHONY: manta-scripts
manta-scripts: deps/manta-scripts/.git
	mkdir -p $(BUILD)/scripts
	cp deps/manta-scripts/*.sh $(BUILD)/scripts

.PHONY: test-unit
test-unit: $(STAMP_NODE_MODULES) | $(NPM_EXEC) $(TAP_EXEC)
	$(TAP_EXEC) -j10 -o test.tap test/unit/*.test.js

.PHONY: test
test:
	@echo "To run all tests, run the following from /opt/smartdc/muskie on a webapi VM:"
	@echo ""
	@echo "    ./test/runtests"
	@exit 1

.PHONY: release
release: all docs
	@echo "Building $(RELEASE_TARBALL)"
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/boot
	@mkdir -p $(RELSTAGEDIR)/site
	@touch $(RELSTAGEDIR)/site/.do-not-delete-me
	@mkdir -p $(RELSTAGEDIR)/root
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/etc
	cp -r \
	    $(ROOT)/build \
	    $(ROOT)/bin \
	    $(ROOT)/boot \
	    $(ROOT)/main.js \
	    $(ROOT)/lib \
	    $(ROOT)/node_modules \
	    $(ROOT)/package.json \
	    $(ROOT)/package-lock.json \
	    $(ROOT)/sapi_manifests \
	    $(ROOT)/smf \
	    $(ROOT)/test \
	    $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)
	cp $(ROOT)/etc/haproxy.cfg.in \
	    $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/etc/
	mv $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/build/scripts \
	    $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/boot
	ln -s /opt/smartdc/$(NAME)/boot/setup.sh \
	    $(RELSTAGEDIR)/root/opt/smartdc/boot/setup.sh
	ln -s ../node_modules/storinfo/bin/mchoose \
		$(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/bin/mchoose
	chmod 755 $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/boot/setup.sh
	cd $(RELSTAGEDIR) && $(TAR) -I pigz -cf $(ROOT)/$(RELEASE_TARBALL) root site
	@rm -rf $(RELSTAGEDIR)

.PHONY: publish
publish: release
	mkdir -p $(ENGBLD_BITS_DIR)/$(NAME)
	cp $(ROOT)/$(RELEASE_TARBALL) $(ENGBLD_BITS_DIR)/$(NAME)/$(RELEASE_TARBALL)

#
# The origin image for muskie has Python 3 as the default version of Python.
# However, this repo depends on javascriptlint, which doesn't build on Python
# 3.  So we create a symlink to Python 2 and add that to our PATH.
#
.PHONY: python2-symlink
python2-symlink:
	mkdir -p $(TOP)/build/agent-python
	if [ -f /opt/local/bin/python2 ]; then \
	    rm -f $(TOP)/build/agent-python/python; \
	    ln -s /opt/local/bin/python2 $(TOP)/build/agent-python/python; \
	fi

include ./deps/eng/tools/mk/Makefile.deps
ifeq ($(shell uname -s),SunOS)
	include ./deps/eng/tools/mk/Makefile.node_prebuilt.targ
	include ./deps/eng/tools/mk/Makefile.agent_prebuilt.targ
	include ./deps/eng/tools/mk/Makefile.smf.targ
endif
include ./deps/eng/tools/mk/Makefile.node_modules.targ
include ./deps/eng/tools/mk/Makefile.targ
