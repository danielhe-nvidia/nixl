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
#include "common/nixl_log.h"
#include <absl/strings/str_format.h>
#define __LOG_ERR(format, ...) { NIXL_ERROR << absl::StrFormat("GUSLI: %s() %s[%d]" format, __PRETTY_FUNCTION__, __FILE__, __LINE__, ##__VA_ARGS__); } while (0)
#define __LOG_DBG(format, ...) { NIXL_DEBUG << absl::StrFormat("GUSLI: " format, ##__VA_ARGS__); } while (0)
#define __LOG_TRC(format, ...) { NIXL_TRACE << absl::StrFormat("GUSLI: " format, ##__VA_ARGS__); } while (0)
#define __LOG_RETERR(rv, format, ...) do { \
	__LOG_ERR("error=%d, " format, (int)rv, ##__VA_ARGS__); \
	return rv; \
} while (0)

nixlGusliEngine::nixlGusliEngine(const nixlBackendInitParams* np) : nixlBackendEngine(np) {
	lib = &gusli::global_clnt_context::get();
	gp.log = stdout;							// Redirect gusli logs to stdout
	if (np && np->customParams) {
		const nixl_b_params_t* bp = np->customParams;
		if (bp->count("client_name") > 0)
			gp.client_name = bp->at("client_name").c_str();
		if (bp->count("max_num_simultaneous_requests") > 0)
			gp.max_num_simultaneous_requests = std::stoi(bp->at("max_num_simultaneous_requests"));
		if (bp->count("config_file") > 0)
			gp.config_file = bp->at("config_file").c_str();
	}
	const int rv = lib->init(gp);
	this->initErr = (rv != 0);
	if (this->initErr) {
		__LOG_ERR("Error opening Gusli driver rv=%d", rv);
		lib = nullptr;
	}
}

nixlGusliEngine::~nixlGusliEngine() {
	if (lib) {
		const int rv = lib->destroy();
		lib = nullptr;
		if (rv)
			__LOG_ERR("Error closing Gusli driver rv=%d", rv);
	}
}

static nixl_status_t __err_conv(const gusli::connect_rv rv) {	// Convert conenction error
	if (rv == gusli::connect_rv::C_OK)              return NIXL_SUCCESS;
	if (rv == gusli::connect_rv::C_NO_DEVICE)       return NIXL_ERR_NOT_FOUND;
	if (rv == gusli::connect_rv::C_WRONG_ARGUMENTS) return NIXL_ERR_INVALID_PARAM;
	return NIXL_ERR_BACKEND;
}

nixl_status_t nixlGusliEngine::_open(uint64_t devId) {
	auto it = bdevs.find(devId);
	if (it != bdevs.end()) {
		struct bdev_refcount_t& v = it->second;
		v.ref_count++;
		const gusli::bdev_info& i = v.bi;
		__LOG_DBG("Open: 0x%lx already exists[ref=%d]: fd=%d, name=%s", devId, v.ref_count, i.bdev_descriptor, i.name);
	} else {
		gusli::backend_bdev_id bdev; bdev.set_from(devId);
		const gusli::connect_rv rv = lib->bdev_connect(bdev);
		if (rv != gusli::connect_rv::C_OK)
			__LOG_RETERR(__err_conv(rv), "connect uuid=%.16s rv=%d", bdev.uuid, (int)rv);
		struct bdev_refcount_t v;
		v.ref_count = 1;
		const gusli::bdev_info& i = v.bi;
		lib->bdev_get_info(bdev, &v.bi);
		__LOG_DBG("Open: {bdev uuid=%.16s, fd=%d name=%s, block_size=%u[B], #blocks=0x%lx}", bdev.uuid, i.bdev_descriptor, i.name, i.block_size, i.num_total_blocks);
		bdevs[devId] = v;
	}
	return NIXL_SUCCESS;
}

nixl_status_t nixlGusliEngine::_close(uint64_t devId) {
	auto it = bdevs.find(devId);
	if (it == bdevs.end()) {
		__LOG_DBG("Close: 0x%lx not oppened", devId);
	} else {
		struct bdev_refcount_t& v = it->second;
		const gusli::bdev_info& i = v.bi;
		v.ref_count--;
		if (v.ref_count > 0) {
			__LOG_DBG("Close: 0x%lx still used[ref=%d]: fd=%d, name=%s", devId, v.ref_count, i.bdev_descriptor, i.name);
			return NIXL_SUCCESS;
		}
		gusli::backend_bdev_id bdev; bdev.set_from(devId);
		const gusli::connect_rv rv = lib->bdev_disconnect(bdev);
		if (rv != gusli::connect_rv::C_OK)
			__LOG_RETERR(__err_conv(rv), "disconnect uuid=%.16s rv=%d", bdev.uuid, (int)rv);
		__LOG_DBG("Close: {bdev uuid=%.16s, fd=%d name=%s, block_size=%u[B], #blocks=0x%lx}", bdev.uuid, i.bdev_descriptor, i.name, i.block_size, i.num_total_blocks);
		bdevs.erase(devId);
	}
	return NIXL_SUCCESS;
}

class nixlGusliMetadata : public nixlBackendMD {
 public:
	gusli::backend_bdev_id bdev;
	uint64_t devId;
	std::vector<gusli::io_buffer_t> io_bufs;
	std::string metadata;
	nixl_mem_t mem_type;
	nixlGusliMetadata(const nixlBlobDesc &mem, nixl_mem_t _mem_type) : nixlBackendMD(true) {
		bdev.set_from(mem.devId);
		devId = mem.devId;
		metadata = mem.metaInfo;
		mem_type = _mem_type;
	}
	~nixlGusliMetadata() {}
};

nixl_status_t nixlGusliEngine::registerMem(const nixlBlobDesc &mem, const nixl_mem_t &mem_type, nixlBackendMD* &out) {
	out = nullptr;
	if ((mem_type != DRAM_SEG) && (mem_type != BLK_SEG))		// we register only ram buffers
		__LOG_RETERR(NIXL_ERR_BACKEND, "type not supported %d!=%d", (int)mem_type, (int)DRAM_SEG);
	nixlGusliMetadata *md = new nixlGusliMetadata(mem, mem_type);
	if (!md) __LOG_RETERR(NIXL_ERR_BACKEND, "out of mem");
	__LOG_DBG("register dev[0x%lx].ram_lba[%p].len=0x%lx, mem_type=%u", mem.devId, (void*)mem.addr, mem.len, mem_type);
	md->io_bufs.emplace_back(gusli::io_buffer_t{ .ptr = (void*)mem.addr, .byte_len = mem.len });
	if (mem_type == BLK_SEG) {
		// Todo: LBA of block devices, verify size, extend volume
	} else {
		if (_open(md->devId) != NIXL_SUCCESS)
			return NIXL_ERR_NOT_FOUND;
		const gusli::connect_rv rv = lib->bdev_bufs_register(md->bdev, md->io_bufs);
		if (rv != gusli::connect_rv::C_OK) {
			delete md;
			__LOG_RETERR(__err_conv(rv), "register buf rv=%d, [%p,0x%lx]", (int)rv, (void*)mem.addr, mem.len);
		}
	}
	out = (nixlBackendMD*)md;
	return NIXL_SUCCESS;
}

nixl_status_t nixlGusliEngine::deregisterMem(nixlBackendMD* _md) {
	nixlGusliMetadata *md = (nixlGusliMetadata *)_md;
	if (!md) __LOG_RETERR(NIXL_ERR_INVALID_PARAM, "md==null");
	__LOG_DBG("unregister dev[0x%lx].ram_lba[%p].len=0x%lx, mem_type=%u", md->devId, (void*)md->io_bufs[0].ptr, md->io_bufs[0].byte_len, md->mem_type);
	if (md->mem_type == BLK_SEG) {
		// Nothing to do
	} else {
		const gusli::connect_rv rv = lib->bdev_bufs_unregist(md->bdev, md->io_bufs);
		if (rv != gusli::connect_rv::C_OK)
			__LOG_RETERR(__err_conv(rv), "unregister buf rv=%d, [%p,0x%lx]", (int)rv, (void*)md->io_bufs[0].ptr, md->io_bufs[0].byte_len);
		if (_close(md->devId) != NIXL_SUCCESS)
			return NIXL_ERR_NOT_FOUND;
	}
	delete md;
	return NIXL_SUCCESS;
}

/********************************** IO ***************************************/
class nixlGusliBackendReqH : public nixlBackendReqH {
	enum gusli::io_error_codes pollable_async_rv;		// NIXL sctively polls on RV instead of waiting for completion. Prevent race condition of free while completion is running by additional copy of rv
	static void completion_cb(nixlGusliBackendReqH *c) {
		__LOG_TRC("IO[op=%c] o[%p]_done, rv=%d", c->io.params.op, c, c->io.get_error());
		c->pollable_async_rv = c->io.get_error();
	}
 public:
	gusli::io_request io;
	nixlGusliBackendReqH(const nixl_xfer_op_t _op) {
		io.params.set_completion(this, completion_cb);
		io.params.op = (_op == NIXL_WRITE) ? gusli::G_WRITE : gusli::G_READ;
		__LOG_TRC("IO[op=%c] o[%p]_prep", io.params.op, this);
	}
	~nixlGusliBackendReqH() {
		__LOG_TRC("IO[op=%c] o[%p]_free", io.params.op, this);
	}
	nixl_status_t exec(void) {
		const long n_bytes = (int)io.params.buf_size();
		pollable_async_rv = gusli::io_error_codes::E_IN_TRANSFER;
		__LOG_TRC("IO[op=%c] o[%p]start, n_ranges=%u ,size=%lu[KB]", io.params.op, this, io.params.num_ranges(), (n_bytes >> 10));
		io.submit_io();
		return NIXL_IN_PROG;
	}
	nixl_status_t poll_status(void) const {	// Convert gusli error code to nixl
		const enum gusli::io_error_codes rv = pollable_async_rv;
		if (rv == gusli::io_error_codes::E_OK)           return NIXL_SUCCESS;
		if (rv == gusli::io_error_codes::E_IN_TRANSFER)  return NIXL_IN_PROG;
		if (rv == gusli::io_error_codes::E_INVAL_PARAMS) return NIXL_ERR_INVALID_PARAM;
		__LOG_RETERR(NIXL_ERR_BACKEND, "io exec error rv=%d", (int)rv);
	}
};

nixl_status_t nixlGusliEngine::prepXfer(const nixl_xfer_op_t &op,
										const nixl_meta_dlist_t &local,
										const nixl_meta_dlist_t &remote,
										const std::string &remote_agent,
										nixlBackendReqH* &handle,
										const nixl_opt_b_args_t* opt_args) const {
	handle = nullptr;
	// Verify params
	if (strcmp(remote_agent.c_str(), gp.client_name)) __LOG_RETERR(NIXL_ERR_INVALID_PARAM, "Remote(%s) != localAgent(%s)", remote_agent.c_str(), gp.client_name);
	if (local.getType() != DRAM_SEG) __LOG_RETERR(NIXL_ERR_INVALID_PARAM, "Local memory type must be DRAM_SEG, got %d", local.getType());
	if (remote.getType() != BLK_SEG) __LOG_RETERR(NIXL_ERR_INVALID_PARAM, "Remote memory type must be BLK_SEG, got %d", remote.getType());
	if (local.descCount() != remote.descCount()) __LOG_RETERR(NIXL_ERR_INVALID_PARAM, "Mismatch in descriptor counts - local[%d] != remote[%d]", local.descCount(), remote.descCount());

	nixlGusliBackendReqH *req = new nixlGusliBackendReqH(op);
	if (!req) __LOG_RETERR(NIXL_ERR_BACKEND, "out of mem");
	const int n_ranges = remote.descCount();
	const struct bdev_refcount_t& v = bdevs.find(remote[0].devId)->second;
	const int id = v.bi.bdev_descriptor;
	if (n_ranges == 1) {
		req->io.params.init_1_rng(req->io.params.op, id, (uint64_t)remote[0].addr, (uint64_t)local[0].len, (void*)local[0].addr);
	} else {
		gusli::io_multi_map_t* mio = (gusli::io_multi_map_t*)local[0].addr;	// Allocate scatter gather in the first entry
		mio->n_entries = n_ranges - 1;		// First entry is the scatter gather
		if (mio->my_size() > local[0].len) {
			delete req;
			__LOG_RETERR(NIXL_ERR_INVALID_PARAM, "mmap of sg=0x%lx[b] > is too long=0x%lx[b], Enlarge mapping or use shorter transfer list", mio->my_size(), local[0].len);
		}
		for (int i = 1; i < n_ranges; i++) {		// Skip first range
			mio->entries[i-1] = (gusli::io_map_t){
				.data = {.ptr = (void*)local[i].addr, .byte_len = (uint64_t)local[i].len, },
				.offset_lba_bytes = (uint64_t)remote[i].addr };
		}
		req->io.params.init_multi(req->io.params.op, id, *mio);
	}
	(void)opt_args;
	handle = (nixlBackendReqH*)req;
	return NIXL_SUCCESS;
}

nixl_status_t nixlGusliEngine::postXfer(const nixl_xfer_op_t &operation, const nixl_meta_dlist_t &local, const nixl_meta_dlist_t &remote, const std::string &remote_agent,
										nixlBackendReqH* &handle, const nixl_opt_b_args_t* opt_args) const {
	(void)operation; (void)local; (void)remote; (void)remote_agent; (void)opt_args;
	nixlGusliBackendReqH *req = (nixlGusliBackendReqH *)handle;
	if (req)
		return req->exec();
	__LOG_RETERR(NIXL_ERR_INVALID_PARAM, "null handle");
}

nixl_status_t nixlGusliEngine::checkXfer(nixlBackendReqH* handle) const {
	const nixlGusliBackendReqH *req = (const nixlGusliBackendReqH *)handle;
	if (req)
		return req->poll_status();
	__LOG_RETERR(NIXL_ERR_INVALID_PARAM, "null handle");
}

nixl_status_t nixlGusliEngine::releaseReqH(nixlBackendReqH* handle) const {
	if (handle) delete ((nixlGusliBackendReqH *)handle);
	return NIXL_SUCCESS;
}
