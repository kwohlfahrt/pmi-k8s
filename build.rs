use std::env;
use std::path::PathBuf;

fn main() {
    // Get PMIx include and lib paths from pkg-config
    let pmix = pkg_config::Config::new()
        .atleast_version("4.0")
        .probe("pmix")
        .expect("PMIx library not found. Install libpmix-dev or equivalent.");

    // Tell cargo to link against pmix
    println!("cargo:rustc-link-lib=pmix");

    // Add library search paths
    for path in &pmix.link_paths {
        println!("cargo:rustc-link-search=native={}", path.display());
    }

    // Generate bindings
    let builder = bindgen::Builder::default()
        .header_contents(
            "wrapper.h",
            r#"
            #include <pmix.h>
            #include <pmix_server.h>
            "#,
        )
        // Tell bindgen about include paths
        .clang_args(
            pmix
                .include_paths
                .iter()
                .map(|p| format!("-I{}", p.display())),
        )
        // Types we need
        .allowlist_type("pmix_proc_t")
        .allowlist_type("pmix_info_t")
        .allowlist_type("pmix_value_t")
        .allowlist_type("pmix_status_t")
        .allowlist_type("pmix_rank_t")
        .allowlist_type("pmix_nspace_t")
        .allowlist_type("pmix_key_t")
        .allowlist_type("pmix_data_type_t")
        .allowlist_type("pmix_server_module_t")
        .allowlist_type("pmix_byte_object_t")
        .allowlist_type("pmix_data_array_t")
        // Callback function types
        .allowlist_type("pmix_op_cbfunc_t")
        .allowlist_type("pmix_modex_cbfunc_t")
        .allowlist_type("pmix_lookup_cbfunc_t")
        .allowlist_type("pmix_spawn_cbfunc_t")
        .allowlist_type("pmix_info_cbfunc_t")
        .allowlist_type("pmix_server_.*_fn_t")
        // Functions we need
        .allowlist_function("PMIx_server_init")
        .allowlist_function("PMIx_server_finalize")
        .allowlist_function("PMIx_server_register_nspace")
        .allowlist_function("PMIx_server_deregister_nspace")
        .allowlist_function("PMIx_server_register_client")
        .allowlist_function("PMIx_server_deregister_client")
        .allowlist_function("PMIx_server_setup_fork")
        .allowlist_function("PMIx_server_dmodex_request")
        .allowlist_function("PMIx_generate_ppn")
        .allowlist_function("PMIx_generate_regex")
        // Data manipulation macros/functions
        .allowlist_function("PMIx_Data_pack")
        .allowlist_function("PMIx_Data_unpack")
        .allowlist_function("PMIx_Data_copy")
        .allowlist_function("PMIx_Info_.*")
        .allowlist_function("PMIx_Proc_.*")
        .allowlist_function("PMIx_Value_.*")
        // Constants
        .allowlist_var("PMIX_.*")
        // Generate Debug trait
        .derive_debug(true)
        .derive_default(true)
        // Use core types
        .use_core()
        .generate_comments(true)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()));

    let bindings = builder.generate().expect("Unable to generate PMIx bindings");

    // Write bindings to the $OUT_DIR/pmix_bindings.rs file
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("pmix_bindings.rs"))
        .expect("Couldn't write PMIx bindings!");
}
