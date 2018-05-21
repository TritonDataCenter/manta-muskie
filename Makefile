#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2017, Joyent, Inc.
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

JS_FILES :=		bin/mlocate \
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
NAME			= muskie
NODE_PREBUILT_TAG	= zone
NODE_PREBUILT_VERSION	:= v4.9.0
# sdc-minimal-multiarch-lts 15.4.1
NODE_PREBUILT_IMAGE	= 04a48d7d-6bb5-4e83-8c3b-e60a99e0f48f

include ./tools/mk/Makefile.defs
include ./tools/mk/Makefile.node_prebuilt.defs
include ./tools/mk/Makefile.node_modules.defs
include ./tools/mk/Makefile.smf.defs

#
# MG Variables
#
RELEASE_TARBALL :=	$(NAME)-pkg-$(STAMP).tar.bz2
ROOT :=			$(shell pwd)
RELSTAGEDIR :=		/tmp/$(STAMP)

#
# Repo-specific targets
#
.PHONY: all
all: $(SMF_MANIFESTS) $(STAMP_NODE_MODULES) manta-scripts

.PHONY: manta-scripts
manta-scripts: deps/manta-scripts/.git
	mkdir -p $(BUILD)/scripts
	cp deps/manta-scripts/*.sh $(BUILD)/scripts

.PHONY: test
test: $(STAMP_NODE_MODULES)
	$(NODE) ./node_modules/.bin/nodeunit --reporter=tap \
	    test/*.test.js test/mpu/*.test.js

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
	chmod 755 $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/boot/setup.sh
	cd $(RELSTAGEDIR) && $(TAR) -jcf $(ROOT)/$(RELEASE_TARBALL) root site
	@rm -rf $(RELSTAGEDIR)

.PHONY: publish
publish: release
	@if [[ -z "$(BITS_DIR)" ]]; then \
		@echo "error: 'BITS_DIR' must be set for 'publish' target"; \
		exit 1; \
	fi
	mkdir -p $(BITS_DIR)/$(NAME)
	cp $(ROOT)/$(RELEASE_TARBALL) $(BITS_DIR)/$(NAME)/$(RELEASE_TARBALL)

include ./tools/mk/Makefile.deps
include ./tools/mk/Makefile.node_prebuilt.targ
include ./tools/mk/Makefile.node_modules.targ
include ./tools/mk/Makefile.smf.targ
include ./tools/mk/Makefile.targ
