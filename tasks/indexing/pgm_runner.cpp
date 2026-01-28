#include <algorithm>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include <pgm/pgm_index.hpp>

using KeyType = uint64_t;

template <typename T>
static void print_percentiles(const std::string &label, std::vector<T> values) {
    if (values.empty()) {
        return;
    }
    std::sort(values.begin(), values.end());
    auto pick = [&](double p) -> T {
        size_t idx = static_cast<size_t>(p * (values.size() - 1));
        return values[idx];
    };
    std::cout << label << "_min:" << values.front() << std::endl;
    std::cout << label << "_p25:" << pick(0.25) << std::endl;
    std::cout << label << "_p50:" << pick(0.50) << std::endl;
    std::cout << label << "_p75:" << pick(0.75) << std::endl;
    std::cout << label << "_p90:" << pick(0.90) << std::endl;
    std::cout << label << "_p95:" << pick(0.95) << std::endl;
    std::cout << label << "_p99:" << pick(0.99) << std::endl;
    std::cout << label << "_max:" << values.back() << std::endl;
}

static std::string get_arg(int argc, char **argv, const std::string &name, const std::string &def) {
    std::string prefix = "--" + name + "=";
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.rfind(prefix, 0) == 0) {
            return arg.substr(prefix.size());
        }
        if (arg == "--" + name && i + 1 < argc) {
            return argv[i + 1];
        }
    }
    return def;
}

static long long get_ll(int argc, char **argv, const std::string &name, long long def) {
    auto value = get_arg(argc, argv, name, "");
    if (value.empty()) {
        return def;
    }
    return std::stoll(value);
}

static int get_int(int argc, char **argv, const std::string &name, int def) {
    auto value = get_arg(argc, argv, name, "");
    if (value.empty()) {
        return def;
    }
    return std::stoi(value);
}

static std::vector<KeyType> read_keys(const std::string &path, size_t count, int has_size) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        throw std::runtime_error("failed to open keys_file");
    }
    if (has_size == 1) {
        uint64_t _size = 0;
        in.read(reinterpret_cast<char *>(&_size), sizeof(uint64_t));
    }
    std::vector<KeyType> keys(count);
    in.read(reinterpret_cast<char *>(keys.data()), static_cast<std::streamsize>(count * sizeof(KeyType)));
    size_t read_count = static_cast<size_t>(in.gcount() / sizeof(KeyType));
    keys.resize(read_count);
    if (keys.empty()) {
        throw std::runtime_error("no keys loaded");
    }
    return keys;
}

static std::vector<KeyType> read_queries(const std::string &path, int has_size) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        throw std::runtime_error("failed to open query_file");
    }
    std::vector<KeyType> queries;
    if (has_size == 1) {
        uint64_t size = 0;
        in.read(reinterpret_cast<char *>(&size), sizeof(uint64_t));
        queries.resize(size);
        in.read(reinterpret_cast<char *>(queries.data()), static_cast<std::streamsize>(size * sizeof(KeyType)));
    } else {
        in.seekg(0, std::ios::end);
        std::streamsize file_size = in.tellg();
        in.seekg(0, std::ios::beg);
        size_t count = static_cast<size_t>(file_size / sizeof(KeyType));
        queries.resize(count);
        in.read(reinterpret_cast<char *>(queries.data()), static_cast<std::streamsize>(count * sizeof(KeyType)));
        if (!queries.empty() && queries[0] == queries.size() - 1) {
            queries.erase(queries.begin());
        }
    }
    if (queries.empty()) {
        throw std::runtime_error("no queries loaded");
    }
    return queries;
}

static std::vector<double> build_zipf_cdf(size_t n, double alpha) {
    std::vector<double> cdf;
    cdf.reserve(n);
    double total = 0.0;
    for (size_t i = 0; i < n; ++i) {
        total += 1.0 / std::pow(static_cast<double>(i + 1), alpha);
        cdf.push_back(total);
    }
    return cdf;
}

static std::vector<KeyType> sample_queries_uniform(const std::vector<KeyType> &keys, size_t count, size_t max_index) {
    std::vector<KeyType> out;
    out.reserve(count);
    std::mt19937_64 gen(19937);
    std::uniform_int_distribution<size_t> dis(0, max_index);
    for (size_t i = 0; i < count; ++i) {
        out.push_back(keys[dis(gen)]);
    }
    return out;
}

static std::vector<KeyType> sample_queries_zipf(const std::vector<KeyType> &keys, size_t count, size_t max_index) {
    std::vector<KeyType> out;
    out.reserve(count);
    std::mt19937_64 gen(19937);
    auto cdf = build_zipf_cdf(max_index + 1, 2.0);
    double total = cdf.back();
    std::uniform_real_distribution<double> dis(0.0, total);
    for (size_t i = 0; i < count; ++i) {
        double r = dis(gen);
        auto it = std::lower_bound(cdf.begin(), cdf.end(), r);
        size_t idx = static_cast<size_t>(std::distance(cdf.begin(), it));
        if (idx > max_index) {
            idx = max_index;
        }
        out.push_back(keys[idx]);
    }
    return out;
}

int main(int argc, char **argv) {
    try {
        std::string keys_file = get_arg(argc, argv, "keys_file", "");
        std::string query_file = get_arg(argc, argv, "query_file", "");
        std::string op_type = get_arg(argc, argv, "op_type", "lookup");
        int case_id = static_cast<int>(get_ll(argc, argv, "case_id", 1));
        int has_size = static_cast<int>(get_ll(argc, argv, "has_size", 0));
        int query_has_size = static_cast<int>(get_ll(argc, argv, "query_has_size", 0));
        int search_count = static_cast<int>(get_ll(argc, argv, "search_count", 10000));
        int r_size = static_cast<int>(get_ll(argc, argv, "r_size", 100));
        int dedupe = get_int(argc, argv, "dedupe", 0);
        size_t total_count = static_cast<size_t>(get_ll(argc, argv, "total_count", 0));

        if (keys_file.empty() || total_count == 0) {
            throw std::runtime_error("keys_file and total_count are required");
        }
        auto keys = read_keys(keys_file, total_count, has_size);
        std::sort(keys.begin(), keys.end());
        if (dedupe) {
            auto end = std::unique(keys.begin(), keys.end());
            keys.erase(end, keys.end());
        }

        std::cout << "start bulk_load..." << std::endl;
        auto build_start = std::chrono::high_resolution_clock::now();
        pgm::PGMIndex<KeyType, 64> index(keys.begin(), keys.end());
        auto build_end = std::chrono::high_resolution_clock::now();
        double build_sec = std::chrono::duration<double>(build_end - build_start).count();
        std::cout << "bulk_load time:" << std::fixed << std::setprecision(6) << build_sec << " sec" << std::endl;

        if (op_type == "bulk") {
            return 0;
        }

        std::vector<KeyType> queries;
        if (!query_file.empty()) {
            queries = read_queries(query_file, query_has_size);
        } else {
            size_t max_index = keys.size() - 1;
            if (op_type == "scan" && r_size > 0 && keys.size() > static_cast<size_t>(r_size)) {
                max_index = keys.size() - static_cast<size_t>(r_size) - 1;
            }
            if (case_id == 1) {
                queries = sample_queries_uniform(keys, static_cast<size_t>(search_count), max_index);
            } else if (case_id == 2) {
                queries = sample_queries_zipf(keys, static_cast<size_t>(search_count), max_index);
            } else {
                throw std::runtime_error("unsupported case_id");
            }
        }

        std::cout << "start to test..." << std::endl;
        std::cout << "start to record..." << std::endl;

        size_t not_found = 0;
        std::vector<long long> latencies;
        std::vector<long long> blocks;
        latencies.reserve(search_count);
        blocks.reserve(search_count);
        volatile KeyType sink = 0;
        auto lookup_start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < search_count; ++i) {
            const KeyType q = queries[static_cast<size_t>(i) % queries.size()];
            auto q_start = std::chrono::high_resolution_clock::now();
            if (op_type == "scan") {
                auto it = std::lower_bound(keys.begin(), keys.end(), q);
                if (it == keys.end()) {
                    not_found++;
                    latencies.push_back(0);
                    blocks.push_back(0);
                    continue;
                }
                size_t idx = static_cast<size_t>(std::distance(keys.begin(), it));
                size_t end = std::min(idx + static_cast<size_t>(r_size), keys.size());
                blocks.push_back(static_cast<long long>(end - idx));
                for (size_t j = idx; j < end; ++j) {
                    sink ^= keys[j];
                }
            } else {
                auto range = index.search(q);
                blocks.push_back(static_cast<long long>(range.hi - range.lo));
                bool found = std::binary_search(keys.begin(), keys.end(), q);
                if (!found) {
                    not_found++;
                }
            }
            auto q_end = std::chrono::high_resolution_clock::now();
            long long q_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    q_end - q_start).count();
            latencies.push_back(q_ns);
        }
        auto lookup_end = std::chrono::high_resolution_clock::now();
        double lookup_sec = std::chrono::duration<double>(lookup_end - lookup_start).count();
        double ops = search_count / lookup_sec;
        double avg_range = 0.0;
        if (!blocks.empty()) {
            long long total = 0;
            for (auto v : blocks) {
                total += v;
            }
            avg_range = static_cast<double>(total) / static_cast<double>(blocks.size());
        }

        std::cout << std::fixed << std::setprecision(4) << ops << " ops" << std::endl;
        std::cout << std::fixed << std::setprecision(4) << avg_range << " block/lookup" << std::endl;
        std::cout << "not found:" << not_found << std::endl;
        print_percentiles("latency_ns", latencies);
        print_percentiles("block", blocks);

        (void)sink;
        return 0;
    } catch (const std::exception &ex) {
        std::cerr << "[error] " << ex.what() << std::endl;
        return 1;
    }
}
