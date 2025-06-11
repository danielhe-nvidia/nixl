/*
 * SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "gusli_backend.h"
#include "common/str_tools.h"
//#include <iostream>

nixlGusliEngine::nixlGusliEngine(const nixlBackendInitParams* np) : nixlBackendEngine(np) {
	lib = &gusli::global_clnt_context::get();
	gusli::global_clnt_context::init_params gp;
	if (np && np->customParams) {
		const nixl_b_params_t* gp = np->customParams;
		if (gp->count("client_name") > 0)
			p.client_name = gp->at("client_name").c_str();
		if (gp->count("max_num_simultaneous_requests") > 0)
			p.max_num_simultaneous_requests = std::stoi(gp->at("max_num_simultaneous_requests"));
		if (gp->count("config_file") > 0)
			p.config_file = gp->at("config_file").c_str();
	}
	const int rv = lib->init(p);
	this->initErr = (rv != 0);
	if (this->initErr) {
		GUSLI_LOG_ERR("Error opening Gusli driver rv=%d", rv);
		lib = nullptr;
	}
}

nixlGusliEngine::~nixlGusliEngine() {
	if (lib) {
		const int rv = lib->destroy();
		lib = nullptr;
		if (rv)
			GUSLI_LOG_ERR("Error closing Gusli driver rv=%d", rv);
	}
}

class nixlGusliMetadata : public nixlBackendMD {
 public:
	gusli::backend_bdev_id bdev;
	std::vector<gusli::io_buffer_t> io_bufs;
	std::string metadata;
	nixlGusliMetadata() : nixlBackendMD(true) {}
	~nixlGusliMetadata() {}
};

GUSLITODO: open_bdev by uuid, where to take it? which function to call
GUSLITODO: register memory how, how to ensure io
GUSLITODO: prepXfer - how to guarantee mmap area
GUSLITODO: How does NIXL handle multiple block devices served by a single driver?
GUSLITODO: singleXfer to 1 block device assumption, is it ok?
GUSLITODO: How to extract the file descriptor of the iostreams so Gusli will flush its logs into the same file descriptor.

		mem.emplace_back(gusli::io_buffer_t{ .ptr = my_io.io_buf, .byte_len = my_io.buf_size });

nixl_status_t nixlGusliEngine::registerMem(const nixlBlobDesc &mem, const nixl_mem_t &nixl_mem, nixlBackendMD* &out) {
	if (nixl_mem != BLK_SEG)
		GUSLI_LOG_RETURN(NIXL_ERR_BACKEND, "type not supported %d", (int)nixl_mem);
	nixlGusliMetadata *md = new nixlGusliMetadata();
	if (!md)
		GUSLI_LOG_RETURN(NIXL_ERR_BACKEND, "out of mem");
	md->bdev = mem.devId; // GUSLITODO
	md->metadata = mem.metaInfo;
	md->io_bufs.emplace_back(gusli::io_buffer_t{ .ptr = (void*)mem.addr, .byte_len = mem.len });
	const gusli::connect_rv rv = lib->bdev_bufs_register(md->bdev, md->io_bufs);
	if (rv != gusli::connect_rv::C_OK) {
		delete md;
		GUSLI_LOG_RETURN(NIXL_ERR_BACKEND, "register buf rv=%d, [%p,0x%lx]", (int)rv, (void*)mem.addr, mem.len);
	}
	out = (nixlBackendMD*)md;
	return NIXL_SUCCESS;
}

nixl_status_t nixlGusliEngine::deregisterMem(nixlBackendMD* _md) {
	nixlGusliMetadata *md = (nixlGusliMetadata *)_md;
	const gusli::connect_rv rv = lib->bdev_bufs_unregist(md->bdev, md->io_bufs);
	//const gusli::connect_rv rv = lib->bdev_disconnect(md->bdev);		GUSLITODO
	if (rv != gusli::connect_rv::C_OK) {
		GUSLI_LOG_RETURN(NIXL_ERR_BACKEND, "unregister buf rv=%d, [%p,0x%lx]", (int)rv, (void*)md->io_bufs[0].ptr, md->io_bufs[0].byte_len);
	}
	delete md;
	return NIXL_SUCCESS;
}

nixl_status_t nixlGusliEngine::prepXfer(const nixl_xfer_op_t &operation,
										const nixl_meta_dlist_t &local,
										const nixl_meta_dlist_t &remote,
										const std::string &remote_agent,
										nixlBackendReqH* &handle,
										const nixl_opt_b_args_t* opt_args) const {
	// Verify params
	if (remote_agent != local_agent) GUSLI_LOG_RETURN(NIXL_ERR_INVALID_PARAM, "Remote(%s) != localAgent(%s)", local_agent, remote_agent);
	if (local.getType() != DRAM_SEG) GUSLI_LOG_RETURN(NIXL_ERR_INVALID_PARAM, "Local memory type must be DRAM_SEG, got %d", local.getType());
	if (remote.getType() != BLK_SEG) GUSLI_LOG_RETURN(NIXL_ERR_INVALID_PARAM, "Remote memory type must be BLK_SEG, got %d", remote.getType());
	if (local.descCount() != remote.descCount()) GUSLI_LOG_RETURN(NIXL_ERR_INVALID_PARAM, "Mismatch in descriptor counts - local[%d] != remote[%d]", local.descCount(), remote.descCount());

	nixlGusliBackendReqH *req = new nixlGusliBackendReqH(operation);
	if (!req)
		return NIXL_ERR_BACKEND;
	const int n_ranges = remote.descCount();
	const int bdev_descriptor = remote[0].devId;
	if (n_ranges <= 1) {
		req->io.params.init_1_rng(io.params.op, bdev_descriptor, (uint64_t)remote[i].addr, (uint64_t)local[i].len, (void*)local[i].addr);
	} else {
		gusli::io_multi_map_t* mio = (gusli::io_multi_map_t*)nullptr;		// Take ?????
		mio->n_entries = remote.descCount();
		for (int i = 0; i < n_ranges; i++) {
			mio->entries[i] = (gusli::io_map_t){
				.data = {.ptr = (void*)local[i].addr, .byte_len = (uint64_t)local[i].len, },
				.offset_lba_bytes = (uint64_t)remote[i].addr };
		}
		my_io.io.params.init_multi(io.params.op, bdev_descriptor, *mio);
	}
	(opt_args);
	handle = (nixlBackendReqH*)req;
	return NIXL_SUCCESS;
}

nixl_status_t nixlGusliEngine::postXfer(const nixl_xfer_op_t &operation, const nixl_meta_dlist_t &local, const nixl_meta_dlist_t &remote, const std::string &remote_agent,
										nixlBackendReqH* &handle, const nixl_opt_b_args_t* opt_args) const {
	(void)operation; (void)local; (void)remote; (void)remote_agent; (void)opt_args;
	nixlGusliBackendReqH *req = (nixlGusliBackendReqH *)handle;
	if (req) {
		req->io.exec();
		return NIXL_IN_PROG;
	}
	GUSLI_LOG_RETURN(NIXL_ERR_INVALID_PARAM, "null handle");
}

nixl_status_t nixlGusliEngine::checkXfer(nixlBackendReqH* handle) const {
	nixlGusliBackendReqH *req = (nixlGusliBackendReqH *)handle;
	if (req) {	// Convert gusli error code to nixl
		const enum gusli::io_error_codes rv = io.get_error();
		if (rv  == gusli::io_error_codes::E_OK) return NIXL_SUCCESS;
		if (rv  == gusli::io_error_codes::E_IN_TRANSFER) return NIXL_IN_PROG;
		if (rv  == gusli::io_error_codes::E_INVAL_PARAMS) return NIXL_ERR_INVALID_PARAM;
			GUSLI_LOG_RETURN(NIXL_ERR_BACKEND, "io exec error rv=%d", (int)rv);
	}
	GUSLI_LOG_RETURN(NIXL_ERR_INVALID_PARAM, "null handle");
}

nixl_status_t nixlGusliEngine::releaseReqH(nixlBackendReqH* handle) const {
	if (handle) delete ((nixlGusliBackendReqH *)handle);
	return NIXL_SUCCESS;
}
