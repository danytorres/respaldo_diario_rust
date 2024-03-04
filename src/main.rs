use aws_config::BehaviorVersion;
use aws_sdk_s3::types::Object;
use aws_sdk_s3::Client;

struct CopyFromTo {
    from: String,
    to: String,
}

async fn retrive_objects(
    client: &Client,
    list_of_all_objects: &mut Vec<Object>,
    prefix_object: String,
    bucket_work: &str,
) -> () {
    let result_list_of_objects = client
        .list_objects()
        .bucket(bucket_work)
        .prefix(prefix_object)
        .send()
        .await;

    match result_list_of_objects {
        Ok(list_of_objects) => match list_of_objects.contents {
            Some(list_object) => {
                list_of_all_objects.extend(list_object);
            }
            None => println!("error No se presenta informacion de los objetos"),
        },
        Err(err) => println!("error {}", err),
    }
}

async fn delete_objects(client: Client, key: String, bucket_work: &str) -> () {
    let object_delete = client
        .delete_object()
        .bucket(bucket_work)
        .key(&key)
        .send()
        .await;

    match object_delete {
        Ok(_) => println!("Se Borro el key: {}", key),
        Err(err) => println!("Se produjo el siguiente error al borrar el objeto: {}", err),
    }
}

async fn copy_objects(client: Client, key_from_to: CopyFromTo, bucket_work: &str) {
    let input_key_copy = format!("{}/{}", bucket_work, &key_from_to.from);
    let result_copy_object = client
        .copy_object()
        .bucket(bucket_work)
        .copy_source(input_key_copy)
        .key(&key_from_to.to)
        .send()
        .await;

    match result_copy_object {
        Ok(_) => println!("Se copio el objeto: {}", key_from_to.to),
        Err(err) => println!(
            "Error en el copiado del objeto: {} hacia {} con el siguiente error {}",
            key_from_to.from, key_from_to.to, err
        ),
    }
}

fn generate_from_to_key(
    list_key_to_copy: Vec<Object>,
    prefix_copy_folder_erp: &str,
    prefix_copy_folder_cap: &str,
    prefix_folder_backup: &str,
) -> Vec<CopyFromTo> {
    let mut list_result: Vec<CopyFromTo> = Vec::new();
    for object_key in list_key_to_copy {
        match object_key.key {
            Some(key) => {
                let key_erp = key.clone().find(prefix_copy_folder_erp);
                let key_to;
                match key_erp {
                    Some(_) => {
                        key_to =
                            key.clone()
                                .replacen(prefix_copy_folder_erp, prefix_folder_backup, 1)
                    }
                    None => {
                        key_to =
                            key.clone()
                                .replacen(prefix_copy_folder_cap, prefix_folder_backup, 1)
                    }
                }

                list_result.push(CopyFromTo {
                    from: key,
                    to: key_to,
                });
            }
            None => println!("No se tiene un key"),
        }
    }
    list_result
}

#[tokio::main]
async fn main() -> () {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .profile_name("palacio")
        .region("us-east-1")
        .load()
        .await;
    let s3_client = Client::new(&config);

    let bucket_work = "eph-datalake-dev-staging";
    let prefix_folder_backup = "Repo-BKPs-Ventas/Respaldo_Repesca_Diaria/";
    let prefix_copy_folder_erp = "sap/erp/";
    let prefix_copy_folder_cap = "sap/cap/";
    let table_cap = ["posdw_tlogf_x/", "posdw_tlogf/", "posdw_tstat/"];
    let table_erp = ["vbrk/", "vbrp/"];

    let mut list_prefix_to_delete =
        Vec::from(table_cap.map(|v| format!("{}{}", prefix_folder_backup, v)));

    list_prefix_to_delete.extend(table_erp.map(|v| format!("{}{}", prefix_folder_backup, v)));

    let mut list_key_to_delete: Vec<Object> = Vec::new();

    for prefix_object in list_prefix_to_delete {
        retrive_objects(
            &s3_client,
            &mut list_key_to_delete,
            prefix_object,
            bucket_work,
        )
        .await;
    }

    let mut concurrent_task: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    for object_name in list_key_to_delete {
        match object_name.key {
            Some(key) => {
                let result_of_task =
                    tokio::spawn(delete_objects(s3_client.clone(), key, &bucket_work));
                concurrent_task.push(result_of_task)
            }
            None => println!("No retorno un key"),
        }
    }

    for task in concurrent_task {
        match task.await {
            Ok(_) => println!("Termino task"),
            Err(err) => println!("error {}", err),
        }
    }

    let mut list_prefix_to_copy: Vec<String> =
        Vec::from(table_cap.map(|v| format!("{}{}", prefix_copy_folder_cap, v)));

    list_prefix_to_copy.extend(table_erp.map(|v| format!("{}{}", prefix_copy_folder_erp, v)));

    let mut list_key_to_copy: Vec<Object> = Vec::new();

    for prefix in list_prefix_to_copy {
        retrive_objects(&s3_client, &mut list_key_to_copy, prefix, bucket_work).await;
    }

    let list_key_from_to = generate_from_to_key(
        list_key_to_copy,
        prefix_copy_folder_erp,
        prefix_copy_folder_cap,
        prefix_folder_backup,
    );

    let mut concurrent_task: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    for key_from_to in list_key_from_to {
        concurrent_task.push(tokio::spawn(copy_objects(
            s3_client.clone(),
            key_from_to,
            bucket_work,
        )));
    }

    for task in concurrent_task {
        match task.await {
            Ok(_) => {
                println!("Termino Task");
            }
            Err(err) => println!("error {}", err),
        }
    }
}
