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
#define GUSLI_LOG_ERR(format, ...) { NIXL_ERROR << absl::StrFormat("GUSLI: %s() %s[%d]" format, __PRETTY_FUNCTION__, __FILE__, __LINE__, ##__VA_ARGS__); } while (0)
#define GUSLI_LOG_DBG(format, ...) { NIXL_ERROR << absl::StrFormat("GUSLI: " format, ##__VA_ARGS__); } while (0)		// GUSLITODO: NIXL_DEBUG
#define GUSLI_LOG_TRC(format, ...) { NIXL_ERROR << absl::StrFormat("GUSLI: " format, ##__VA_ARGS__); } while (0)		// GUSLITODO: NIXL_TRACE

#define GUSLI_LOG_RETURN(rv, format, ...) do { \
	GUSLI_LOG_ERR("error=%d, " format, (int)rv, ##__VA_ARGS__); \
	return rv; \
} while (0)

nixlGusliEngine::nixlGusliEngine(const nixlBackendInitParams* np) : nixlBackendEngine(np) {
	lib = &gusli::global_clnt_context::get();
	gp.log = stderr;	// GUSLITODO: stdout
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

/*
GUSLITODO: singleXfer to 1 block device assumption, is it ok?
NIXL bench - see how gusli connects there?
Are there special instructions on how to compile and run a unit-test on a laptop? Specific chapel versions, etc
/contrib/build.sh --nixl /Users/vladbu/src/nixl

4. You are doing a loopback plugin, there is no "remote" in your case anyway. During memory registration you can pass the nvme path string via nixlBlobDesc.metaInfo and save a mapping from devId to that string in some internal map if you need it for the following Xfer ops.
Support multi-io to multiple bdevs at once
*/
static nixl_status_t __err_conv(const gusli::connect_rv rv) {	// Convert conenction error
	if (rv == gusli::connect_rv::C_OK)              return NIXL_SUCCESS;
	if (rv == gusli::connect_rv::C_NO_DEVICE)       return NIXL_ERR_NOT_FOUND;
	if (rv == gusli::connect_rv::C_WRONG_ARGUMENTS) return NIXL_ERR_INVALID_PARAM;
	return NIXL_ERR_BACKEND;
}

nixl_status_t nixlGusliEngine::_open(uint64_t devId) {
	auto it = bdevs.find(devId);
	if (it != bdevs.end()) {
		const gusli::bdev_info& i = it->second;
		GUSLI_LOG_DBG("Open: 0x%lx already exists: fd=%d, name=%s", devId, i.bdev_descriptor, i.name);
	} else {
		gusli::backend_bdev_id bdev; bdev.set_from(devId);
		const gusli::connect_rv rv = lib->bdev_connect(bdev);
		if (rv != gusli::connect_rv::C_OK)
			if (rv != gusli::connect_rv::C_ALREADY_CONNECTED)	// NIXL does not have explicit open
				GUSLI_LOG_RETURN(__err_conv(rv), "connect uuid=%s rv=%d", bdev.uuid, (int)rv);
		gusli::bdev_info i;
		lib->bdev_get_info(bdev, &i);
		GUSLI_LOG_DBG("Open: {bdev uuid=%s, fd=%d name=%s, block_size=%u[B], #blocks=0x%lx}", bdev.uuid, i.bdev_descriptor, i.name, i.block_size, i.num_total_blocks);
		bdevs[devId] = i;
	}
	return NIXL_SUCCESS;
}

nixl_status_t nixlGusliEngine::_close(uint64_t devId) {
	auto it = bdevs.find(devId);
	if (it == bdevs.end()) {
		GUSLI_LOG_DBG("Close: 0x%lx not oppened");	// bdev = it->second;
	} else {
		gusli::backend_bdev_id bdev; bdev.set_from(devId);
		const gusli::connect_rv rv = lib->bdev_disconnect(bdev);
		if (rv != gusli::connect_rv::C_OK)
			if (rv != gusli::connect_rv::C_WRONG_ARGUMENTS)	// NIXL does not have explicit close
				GUSLI_LOG_RETURN(__err_conv(rv), "disconnect uuid=%s rv=%d", bdev.uuid, (int)rv);
		const gusli::bdev_info& i = it->second;
		GUSLI_LOG_DBG("Close: {bdev uuid=%s, fd=%d name=%s, block_size=%u[B], #blocks=0x%lx}", bdev.uuid, i.bdev_descriptor, i.name, i.block_size, i.num_total_blocks);
		bdevs.erase(devId);
	}
	return NIXL_SUCCESS;
}

nixl_status_t nixlGusliEngine::registerMem(const nixlBlobDesc &mem, const nixl_mem_t &mem_type, nixlBackendMD* &out) {
	out = nullptr;
	if ((mem_type != DRAM_SEG) && (mem_type != BLK_SEG))		// we register only ram buffers
		GUSLI_LOG_RETURN(NIXL_ERR_BACKEND, "type not supported %d!=%d", (int)mem_type, (int)DRAM_SEG);
	nixlGusliMetadata *md = new nixlGusliMetadata(mem, mem_type);
	if (!md)
		GUSLI_LOG_RETURN(NIXL_ERR_BACKEND, "out of mem");
	if (mem_type == BLK_SEG) {		// Todo: LBA of block devices, verify size, extend volume
		GUSLI_LOG_DBG("register dev[0x%lx].blk_lba[%p].len=0x%lx, skipped", mem.devId, (void*)mem.addr, mem.len);
		return NIXL_SUCCESS;
	}
	if (_open(md->devId) != NIXL_SUCCESS)
		return NIXL_ERR_NOT_FOUND;
	GUSLI_LOG_DBG("register dev[0x%lx].ram_lba[%p].len=0x%lx", mem.devId, (void*)mem.addr, mem.len);
	md->io_bufs.emplace_back(gusli::io_buffer_t{ .ptr = (void*)mem.addr, .byte_len = mem.len });
	const gusli::connect_rv rv = lib->bdev_bufs_register(md->bdev, md->io_bufs);
	if (rv != gusli::connect_rv::C_OK) {
		delete md;
		GUSLI_LOG_RETURN(__err_conv(rv), "register buf rv=%d, [%p,0x%lx]", (int)rv, (void*)mem.addr, mem.len);
	}
	out = (nixlBackendMD*)md;
	return NIXL_SUCCESS;
}

nixl_status_t nixlGusliEngine::deregisterMem(nixlBackendMD* _md) {
	nixlGusliMetadata *md = (nixlGusliMetadata *)_md;
	if ((!md) || (md->mem_type == BLK_SEG)) {
		return NIXL_SUCCESS;
	}
	const gusli::connect_rv rv = lib->bdev_bufs_unregist(md->bdev, md->io_bufs);
	//if (md) { NIXL_ERROR << "NNNNNNNNNNmmmmyy11111 p=" << (void*)_md << " mem=" <<(int)md->mem_type << " devId= " << md->devId << " rv=" << (int)rv; }
	if (rv != gusli::connect_rv::C_OK)
		GUSLI_LOG_RETURN(__err_conv(rv), "unregister buf rv=%d, [%p,0x%lx]", (int)rv, (void*)md->io_bufs[0].ptr, md->io_bufs[0].byte_len);
	if (_close(md->devId) != NIXL_SUCCESS)
		return NIXL_ERR_NOT_FOUND;
	delete md;
	return NIXL_SUCCESS;
}

/********************************** IO ***************************************/
class nixlGusliBackendReqH : public nixlBackendReqH {
	static void completion_cb(nixlGusliBackendReqH *c) {
		GUSLI_LOG_TRC("IO[op=%c] o[%p]_done, rv=%d", c->io.params.op, c, c->io.get_error());
		c->pollable_async_rv = c->io.get_error();
	}
 public:
	gusli::io_request io;
	enum gusli::io_error_codes pollable_async_rv;		// NIXLsactively polls on RV instead of waiting for completion. Prevent race condition of free while completion is running by additional copy of rv
	nixlGusliBackendReqH(const nixl_xfer_op_t _op) {
		io.params.set_completion(this, completion_cb);
		io.params.op = (_op == NIXL_WRITE) ? gusli::G_WRITE : gusli::G_READ;
		GUSLI_LOG_TRC("IOprep op=%c", io.params.op);
	}
	~nixlGusliBackendReqH() {
		GUSLI_LOG_TRC("IOfree");
	}
	nixl_status_t exec(void) {
		const long n_bytes = (int)io.params.buf_size();
		pollable_async_rv = gusli::io_error_codes::E_IN_TRANSFER;
		GUSLI_LOG_TRC("IO[op=%c] o[%p]start, n_ranges=%u ,size=%lu[KB]", io.params.op, this, io.params.num_ranges(), (n_bytes >> 10));
		io.submit_io();
		return NIXL_IN_PROG;
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
	if (strcmp(remote_agent.c_str(), gp.client_name)) GUSLI_LOG_RETURN(NIXL_ERR_INVALID_PARAM, "Remote(%s) != localAgent(%s)", remote_agent.c_str(), gp.client_name);
	if (local.getType() != DRAM_SEG) GUSLI_LOG_RETURN(NIXL_ERR_INVALID_PARAM, "Local memory type must be DRAM_SEG, got %d", local.getType());
	if (remote.getType() != BLK_SEG) GUSLI_LOG_RETURN(NIXL_ERR_INVALID_PARAM, "Remote memory type must be BLK_SEG, got %d", remote.getType());
	if (local.descCount() != remote.descCount()) GUSLI_LOG_RETURN(NIXL_ERR_INVALID_PARAM, "Mismatch in descriptor counts - local[%d] != remote[%d]", local.descCount(), remote.descCount());

	nixlGusliBackendReqH *req = new nixlGusliBackendReqH(op);
	if (!req)
		return NIXL_ERR_BACKEND;
	const int n_ranges = remote.descCount();
	const gusli::bdev_info& i = bdevs.find(remote[0].devId)->second;
	const int bdev_descriptor = i.bdev_descriptor;
	if (n_ranges <= 1) {
		req->io.params.init_1_rng(req->io.params.op, bdev_descriptor, (uint64_t)remote[0].addr, (uint64_t)local[0].len, (void*)local[0].addr);
	} else {
		gusli::io_multi_map_t* mio = (gusli::io_multi_map_t*)local[0].addr;
		mio->n_entries = local.descCount() - 1;		// First entry is the scatter gather
		if (mio->my_size() > local[0].len)
			GUSLI_LOG_RETURN(NIXL_ERR_INVALID_PARAM, "mmap of sg=0x%lx[b] > is too long=0x%lx[b], Enlarge mapping or use shorter transfer list", mio->my_size(), local[0].len);
		for (int i = 1; i < n_ranges; i++) {		// Skip first range
			mio->entries[i] = (gusli::io_map_t){
				.data = {.ptr = (void*)local[i].addr, .byte_len = (uint64_t)local[i].len, },
				.offset_lba_bytes = (uint64_t)remote[i].addr };
		}
		req->io.params.init_multi(req->io.params.op, bdev_descriptor, *mio);
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
	GUSLI_LOG_RETURN(NIXL_ERR_INVALID_PARAM, "null handle");
}

nixl_status_t nixlGusliEngine::checkXfer(nixlBackendReqH* handle) const {
	nixlGusliBackendReqH *req = (nixlGusliBackendReqH *)handle;
	if (req) {	// Convert gusli error code to nixl
		const enum gusli::io_error_codes rv = req->pollable_async_rv;
		if (rv == gusli::io_error_codes::E_OK) return NIXL_SUCCESS;
		if (rv == gusli::io_error_codes::E_IN_TRANSFER) return NIXL_IN_PROG;
		if (rv == gusli::io_error_codes::E_INVAL_PARAMS) return NIXL_ERR_INVALID_PARAM;
			GUSLI_LOG_RETURN(NIXL_ERR_BACKEND, "io exec error rv=%d", (int)rv);
	}
	GUSLI_LOG_RETURN(NIXL_ERR_INVALID_PARAM, "null handle");
}

nixl_status_t nixlGusliEngine::releaseReqH(nixlBackendReqH* handle) const {
	if (handle) delete ((nixlGusliBackendReqH *)handle);
	return NIXL_SUCCESS;
}
