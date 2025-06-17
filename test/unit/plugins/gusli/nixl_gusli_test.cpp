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
#include <iostream>			// std:cerr
#include <iomanip>			// std::setprecision
#include <unistd.h>
#include <stdlib.h>
#include <absl/strings/str_format.h>
#include "nixl.h"
#include "common/nixl_time.h"
#include <getopt.h>

#ifndef __stringify
	#define __stringify_1(x...)	#x
	#define __stringify(x...)	__stringify_1(x)
#endif

namespace {
	std::ostream& err_log = std::cerr;
	std::ostream& out_log = std::cout;
};
class gtest {		// Gusli tester class
 private:
	static constexpr const size_t gb_size = (1 << 30);
	static constexpr const int line_width = 60;					// Unites typical line width
	static constexpr const char* agent_name = "GUSLITester";
	static constexpr const char* line_str = "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";
	int num_transfers;
	size_t transfer_size;
	const size_t bdev_byte_offset = (1UL << 20);	// Write at offset 1[MB] in block device
	long page_size;
	long sg_buf_size;			// Array of pages for describing the list of io descriptors in registred memory
	void* ptr;					// Registered mem RAM buffer for ios
	nixlXferReqH* treq = nullptr;			// io request

	static std::string center_str(const std::string& str) { return std::string((line_width - str.length()) / 2, ' ') + str; }
	static bool test_pattern_do(void* buffer, size_t size, const char* action) {
		static constexpr const size_t test_phrase_len = 32;
		char test_phrase[test_phrase_len+1] __attribute__((aligned(sizeof(long))));
		strcpy(test_phrase, "|NIXL bdev 32[b] GUSLI pattern |");
		char* buf = (char*)buffer;
		if (action[0] == 'f') {						// Fill
			for (size_t i = 0; i < size; i += test_phrase_len) {
				*((size_t*)&test_phrase[24]) = i;		// Unique last 64 bits for each 32[b] string
				memcpy(&buf[i], test_phrase, test_phrase_len);
			}
		} else if (action[0] == 'c') {				// Clear
			memset(buffer, 'c', size);
		} else if (action[0] == 'p') {				// Print prefix
			out_log << "BUF: "; out_log.write((char*)buffer, 16); out_log << "\n";
		} else {									// Verify
			for (size_t i = 0; i < size; i += test_phrase_len) {
				*((size_t*)&test_phrase[24]) = i;		// Unique last 64 bits for each 32[b] string
				if (0 != memcmp(&buf[i], test_phrase, test_phrase_len)) {
					err_log << "DRAM buffer[" << i << "] validation failed with error, size=" << size << "\n test=" << test_phrase << "buf=" << buf[i] << "\n";
					return false;
				}
			}
		}
		return true;
	}

	static std::string format_time(nixlTime::us_t us) {	// Helper function to format duration
		const nixlTime::ms_t ms = us/1000.0;
		return (ms < 1000) ?  absl::StrFormat("%.0f[ms]", ms) : absl::StrFormat("%.3f[sec]", ((double)ms / 1000.0));
	}

	static void progress_bar(float p /* [0..1]*/) {
		static constexpr const int progress_bar_width = (line_width - 2); // -2 for the brackets
		out_log << "[";
		int i;
		const int n_chars = (progress_bar_width * p);
		for (i = 0; i < n_chars; ++i) out_log << "=";
		out_log << ">";
		for (     ; i < progress_bar_width; ++i) out_log << " ";
		out_log << absl::StrFormat("] %.1f%% ", (p * 100.0));
		if (p >= 1.0) {	// Add completion indicator
			out_log << "DONE!\n";
		} else {
			out_log << "\r";
			out_log.flush();
		}
	}
	static std::string phase_title(const std::string& title) {
		static int phase_num = 1;
		return absl::StrFormat("PHASE %d: %s", phase_num++, title.c_str());
	}
	static void print_segment_title(const std::string& title) {
		out_log << line_str << center_str(title) << line_str;
	}

 public:
	gtest(int _num_transfers, size_t _transfer_size) : num_transfers(_num_transfers), transfer_size(_transfer_size), page_size(-1), ptr(nullptr) {
		page_size = sysconf(_SC_PAGESIZE);
		if (page_size <= 0) {
			err_log << "Error: Invalid page size returned by sysconf\n";
			return;
		}
		if (num_transfers % 4)					// Make num_transfers aligned to 4
			num_transfers = ((num_transfers + 3) / 4) * 4;
		if ((transfer_size % page_size) != 0) 	// align transfer size to page size
			transfer_size = ((transfer_size + page_size - 1) / page_size) * page_size;
		sg_buf_size = num_transfers*32;		// SG (scatter gather element is 24[b] (ptr+len+offset). Round to 32)
		sg_buf_size = ((sg_buf_size + page_size - 1) / page_size) * page_size;	// Round to page size
	}
	~gtest() {
		if (ptr) free(ptr);
	}

	nixl_b_params_t gen_gusli_plugin_params(void) const {	// Set up backend parameters for gusli::global_clnt_context::init_params
		#define UUID_LOCAL_FILE_0 11
		#define UUID_LOCAL_FILE_1 12
		#define UUID_NVME_DISK__0 17
		nixl_b_params_t params;
		params["client_name"] = agent_name;
		params["config_file"] = "# version=1, bdevs: UUID-16b, type, attach_op, direct, path, security_cookie\n"
			__stringify(UUID_LOCAL_FILE_0) " f W N ./store0.bin sec=0x3\n"		// Local file in non direct mode
			__stringify(UUID_LOCAL_FILE_1) " f W N ./store1.bin sec=0x3\n"		// Local file in non direct mode
			__stringify(UUID_NVME_DISK__0) " K X D /dev/nvme0n1 sec=0x7\n";	// NVME in direct mode
		params["max_num_simultaneous_requests"] = std::to_string(256);
		return params;
	}

	#define QUIT_ON_ERR(msg, status) { if (status < NIXL_SUCCESS) { err_log << "Error: " << msg << nixlEnumStrings::statusStr(status) << std::endl; if (treq) agent.releaseXferReq(treq); return -__LINE__; } } while (0)
	int register_bufs_on_multi_bdev(nixlAgent& agent, bool do_reg) {	// Register the large IO buffer + additional sg on 2 bdevs
		const size_t n_total_mapped_bytes = (num_transfers * transfer_size);
		const char* action_str = (do_reg ? "R" : "Unr");
		nixl_reg_dlist_t dram_reg(DRAM_SEG), bdev_reg(BLK_SEG);
		int bdevs[2] = {UUID_LOCAL_FILE_0, UUID_LOCAL_FILE_1};
		nixlBlobDesc d;
		nixl_status_t status;
		d.devId = 0;
		d.len = n_total_mapped_bytes + sg_buf_size;
		d.addr = (uintptr_t)ptr;   dram_reg.addDesc(d);
		d.addr = bdev_byte_offset; bdev_reg.addDesc(d);
		for (int i = 0; i < 2; i++ ) {
			dram_reg[0].devId = bdev_reg[0].devId = bdevs[i];
			status = (do_reg ? agent.registerMem(dram_reg) : agent.deregisterMem(dram_reg));
			QUIT_ON_ERR(absl::StrFormat("Failed bdev=%u %eg=%s, rv=", bdevs[i], action_str, nixlEnumStrings::memTypeStr(dram_reg.getType())), status);
			progress_bar(i*0.5f + 0.25f);
			status = (do_reg ? agent.registerMem(bdev_reg) : agent.deregisterMem(bdev_reg));
			QUIT_ON_ERR(absl::StrFormat("Failed bdev=%u %eg=%s, rv=", bdevs[i], action_str, nixlEnumStrings::memTypeStr(bdev_reg.getType())), status);
			progress_bar(i*0.5f + 0.50f);
		}
		return 0;
	}

	int run_write_read_verify(void) {
		nixlAgent agent(agent_name, nixlAgentConfig(true));
		nixl_b_params_t params = gen_gusli_plugin_params();

		// Print test configuration information
		const size_t n_total_mapped_bytes = (num_transfers * transfer_size);
		print_segment_title("NIXL STORAGE TEST STARTING (GUSLI PLUGIN)");
		out_log << absl::StrFormat("Configuration:\n");
		out_log << absl::StrFormat("- Number of transfers=%d\n", num_transfers);
		out_log << absl::StrFormat("- Transfer=%zu[KB], sg=%zu[KB]\n", (transfer_size >> 10), (sg_buf_size >> 10));
		out_log << absl::StrFormat("- Total data: %.2f[GB]\n", float(n_total_mapped_bytes) / gb_size);
		out_log << absl::StrFormat("- Backend: GUSLI, Direct IO enabled\n") << line_str;

		// Create GUSLI backend first - before allocating any resources
		nixlBackendH* n_backend = nullptr;		// Backend gusli plugin
		nixl_status_t status;
		status = agent.createBackend("GUSLI", params, n_backend);
		QUIT_ON_ERR("Backend Creation Failed: ", status);

		print_segment_title(phase_title("Allocating buffers, bdev " + UUID_LOCAL_FILE_0));
		if (posix_memalign(&ptr, page_size, n_total_mapped_bytes + sg_buf_size) != 0)
			QUIT_ON_ERR("DRAM allocation failed", NIXL_ERR_NOT_SUPPORTED);
		nixl_xfer_dlist_t bdev_io_src(DRAM_SEG), bdev_io_dst(BLK_SEG);
		{
			nixlBlobDesc d;
			d.devId = UUID_LOCAL_FILE_0;
			// First entry is dummy, with enough space for scatter gather
			d.len = sg_buf_size;
			d.addr = (uintptr_t)((u_int64_t)ptr + n_total_mapped_bytes);
			bdev_io_src.addDesc(d);
			d.addr = bdev_byte_offset;	// Dummy
			bdev_io_dst.addDesc(d);

			for (int i = 0; i < num_transfers; ++i) {		// Create all the transfer
				const size_t io_offset = (i*transfer_size);
				d.len = transfer_size;
				d.addr = (uintptr_t)((size_t)ptr + io_offset);	// Offset in RAM buffer
				bdev_io_src.addDesc(d);
				d.addr = bdev_byte_offset + io_offset;
				bdev_io_dst.addDesc(d);
				progress_bar(float(i + 1) / num_transfers);
			}
		}

		print_segment_title(phase_title("Registering memory with NIXL"));
		nixl_reg_dlist_t dram_reg(DRAM_SEG), bdev_reg(BLK_SEG);
		{	// Register the large IO buffer + additional sg
			nixlBlobDesc d;
			d.devId = bdev_io_src[0].devId;
			d.len = n_total_mapped_bytes;	// Just for debug, register in 2 descriptos, can register as 1 buffer as well
			d.addr = (uintptr_t)ptr; dram_reg.addDesc(d);
			d.len = sg_buf_size;
			d.addr = (uintptr_t)((size_t)ptr + n_total_mapped_bytes); dram_reg.addDesc(d);
			// Just for debug, register in 4 quarters, can register as 1 buffer as well
			d.len = n_total_mapped_bytes/4;	// Security reason: Enforces all IO's within registerd memory. Not needed internally by the plugin
			for (int i = 0; i < 4; i++ ) {
				d.addr = bdev_byte_offset + i*d.len; bdev_reg.addDesc(d);
			}
			status = agent.registerMem(dram_reg);
			QUIT_ON_ERR(absl::StrFormat("Failed reg=%s, rv=", nixlEnumStrings::memTypeStr(dram_reg.getType())), status);
			progress_bar(0.5f);
			status = agent.registerMem(bdev_reg);
			QUIT_ON_ERR(absl::StrFormat("Failed reg=%s, rv=", nixlEnumStrings::memTypeStr(bdev_reg.getType())), status);
			progress_bar(1.0f);
		}

		enum nixl_xfer_op_t io_phases[] = {NIXL_WRITE, NIXL_READ};	// First write - then read
		print_segment_title(phase_title("1[xfer] Write-Read-Verify"));
		{
			nixl_xfer_dlist_t bdev_io_1src(DRAM_SEG), bdev_io_1dst(BLK_SEG);
			bdev_io_1src.addDesc(bdev_io_src[4]);		// Just an arbitrary 4'th io
			bdev_io_1dst.addDesc(bdev_io_dst[4]);
			test_pattern_do((char*)bdev_io_1src[0].addr, transfer_size, "fill");
			for (int i = 0; i < 2; i++, treq = nullptr) {
				const std::string io_t_str = nixlEnumStrings::xferOpStr(io_phases[i]);
				status = agent.createXferReq(io_phases[i], bdev_io_1src, bdev_io_1dst, agent_name, treq);
				QUIT_ON_ERR("Failed to create req, rv=", status);
				status = agent.postXferReq(treq);
				QUIT_ON_ERR("Failed to post req, rv=", status);
				do { // Busy loop wait for transfer to complete
					status = agent.getXferStatus(treq);
					QUIT_ON_ERR("Failed during transfer req, rv=", status);
				} while (status == NIXL_IN_PROG);
				if (io_phases[i] == NIXL_WRITE)		// Clear buffers before read
					test_pattern_do(ptr, transfer_size, "clear");
				agent.releaseXferReq(treq);
			}
			if (!test_pattern_do((char*)bdev_io_1src[0].addr, transfer_size, "verify")) return -__LINE__;
		}

		nixlTime::us_t total_time(0);
		double total_data_gb(0);
		test_pattern_do(ptr, n_total_mapped_bytes, "fill");
		for (int i = 0; i < 2; i++, treq = nullptr) {
			const std::string io_t_str = nixlEnumStrings::xferOpStr(io_phases[i]);
			print_segment_title(phase_title(absl::StrFormat("%s Test, nIOs=%u", io_t_str.c_str(), (int)bdev_io_src.descCount()-1)));
			status = agent.createXferReq(io_phases[i], bdev_io_src, bdev_io_dst, agent_name, treq);
			QUIT_ON_ERR("Failed to create req, rv=", status);
			const nixlTime::us_t time_start = nixlTime::getUs();
			status = agent.postXferReq(treq);
			QUIT_ON_ERR("Failed to post req, rv=", status);
			do { // Busy loop wait for transfer to complete
				status = agent.getXferStatus(treq);
				QUIT_ON_ERR("Failed during transfer req, rv=", status);
			} while (status == NIXL_IN_PROG);
			const nixlTime::us_t time_end = nixlTime::getUs();
			const nixlTime::us_t micro_secs = (time_end - time_start);
			const double data_gb = (double)n_total_mapped_bytes / (double)gb_size;
			out_log << "- Time: " << format_time(micro_secs) << std::endl;
			out_log << "- Data: " << std::fixed << std::setprecision(2) << data_gb << "[GB]\n";
			out_log << "- Speed: " << ((data_gb * 1000000.0) / micro_secs) << "[GB/s]\n";
			total_time += micro_secs;
			total_data_gb += data_gb;
			agent.releaseXferReq(treq);
			if (io_phases[i] == NIXL_WRITE)		// Clear buffers before read
				test_pattern_do(ptr, n_total_mapped_bytes, "clear");
		}
		print_segment_title(phase_title("Validating read data"));
		if (!test_pattern_do(ptr, n_total_mapped_bytes, "verify")) return -5;

		{	print_segment_title(phase_title("Un-Registering memory with NIXL"));
			status = agent.deregisterMem(dram_reg);
			QUIT_ON_ERR(absl::StrFormat("Failed de-reg=%s, rv=", nixlEnumStrings::memTypeStr(dram_reg.getType())), status);
			status = agent.deregisterMem(bdev_reg);
			QUIT_ON_ERR(absl::StrFormat("Failed de-reg=%s, rv=", nixlEnumStrings::memTypeStr(bdev_reg.getType())), status);
		}
		print_segment_title("TEST write-read summary");
		out_log << "Total time: " << format_time(total_time) << std::endl;
		out_log << "Total data: " << std::fixed << std::setprecision(2) << total_data_gb << "[GB]" << line_str;


		print_segment_title("TEST 1-transfer on 2 bdevs");
		if (register_bufs_on_multi_bdev(agent, true ) < 0) return -__LINE__;
		if (register_bufs_on_multi_bdev(agent, false) < 0) return -__LINE__;
		return 0;
	}
};

int main(int argc, char *argv[]) {
	static constexpr const int default_num_transfers = 1024;
	static constexpr const size_t default_transfer_size = (1UL << 19); // 512KB
	int opt, num_transfers = default_num_transfers;
	size_t transfer_size = default_transfer_size;
	while ((opt = getopt(argc, argv, "n:s:h")) != -1) {
		switch (opt) {
			case 'n': num_transfers = std::stoi(  optarg); break;
			case 's': transfer_size = std::stoull(optarg); break;
			case 'h':
			default:
				out_log << absl::StrFormat("Usage: %s [-n num_transfers] [-s transfer_size] [-h]\n", argv[0]);
				out_log << absl::StrFormat("  -n num_transfers      Number of transfers (default: %d)\n", default_num_transfers);
				out_log << absl::StrFormat("  -s transfer_size      Size of each transfer[Bytes] (default: %zu)\n", default_transfer_size);
				out_log << absl::StrFormat("  -h                    Show this help message\n");
				return (opt == 'h') ? 0 : 1;
		}
	}
	gtest base_test(num_transfers, transfer_size);
	const int rv = base_test.run_write_read_verify();
	return rv;
}