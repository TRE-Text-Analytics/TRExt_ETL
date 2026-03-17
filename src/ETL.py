from db import BATCH_SIZE, FETCH_SIZE, flush_domain_buffer, generic_flush, get_checkpoint, get_connections, update_checkpoint
from transform import transform_note, transform_nlp_batch
import logging
import sys

log_filename = "etl_errors.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler(log_filename), # Logs to a file
        logging.StreamHandler(sys.stdout)   # Also still outputs to console for visibility
    ]
)
logger = logging.getLogger("ETL_Runner")

def process_table(src_conn, tgt_conn, table_config):
    """
    A generic runner to handle streaming, transforming, and batching.
    Supports both row-by-row and batch transformation functions.
    """
    name = table_config['name']
    query = table_config['query']
    transform_func = table_config['transform_func']
    flush_func = table_config['flush_func']
    columns = table_config.get('columns') # Optional

    # Determine if we are using the batch processing function
    is_batch_mode = getattr(transform_func, '__name__', '') == 'transform_nlp_batch'

    try:
        with tgt_conn.cursor() as checkpoint_cur:
            last_id = get_checkpoint(checkpoint_cur, name)
    except Exception as e:
        logger.error(f"[{name}] Failed to fetch initial checkpoint: {e}")
        return
    
    print(f"--- Starting {name} from ID {last_id} ---")

    raw_buffer = []      # Holds untransformed rows (only used in batch mode)
    buffer = []          # Contains transformed rows ready for bulk insert
    domain_buffer = []   # Tuple of (domain_table_name, row) for domain-specific inserts
    current_high_id = last_id

    # Inner helper to prevent duplicating the flush logic
    def execute_flush(high_id):
        try:
            with tgt_conn.cursor() as tgt_cur:
                if buffer:
                    flush_func(tgt_cur, buffer, name, columns)
                if domain_buffer:
                    flush_domain_buffer(tgt_cur, domain_buffer)
                update_checkpoint(tgt_cur, name, high_id)
            
                tgt_conn.commit()
                logger.info(f"[{name}] Successfully flushed batch. Checkpoint: {high_id}")
            
        except Exception as e:
            # IMPORTANT: Rollback the transaction on ANY failure
            tgt_conn.rollback()
            logger.error(f"[{name}] Critical error during flush at ID {high_id}. Transaction rolled back.")
            logger.error(f"Error details: {e}")
            # Raise the error to stop the whole ETL process rather than continuing with bad state
            raise 
        finally:
            buffer.clear()
            domain_buffer.clear()
    

    try:
        with src_conn.cursor(name=f'{name}_cursor') as src_cur:
            src_cur.itersize = FETCH_SIZE 
            src_cur.execute(query, (last_id,))

            for row in src_cur:
                current_high_id = row[0] # Assumes ID used for checkpointing is the first col
                try:
                    if is_batch_mode:
                        raw_buffer.append(row)
                        # If we hit the batch size, transform the whole chunk at once
                        if len(raw_buffer) >= BATCH_SIZE:
                            # Pass the target connection and the raw rows to our batch function
                            batch_results = transform_func(tgt_conn, raw_buffer)
                            
                            # Unpack the results into the respective insert buffers
                            for transformed, domain_row in batch_results:
                                if transformed:
                                    buffer.append(transformed)
                                if domain_row:
                                    domain_buffer.append(domain_row)
                            
                            execute_flush(current_high_id)
                            raw_buffer.clear()
                    else:
                        # Standard row-by-row processing
                        transformed, domain_row = transform_func(row)
                        if transformed:
                            buffer.append(transformed)
                        if domain_row:
                            domain_buffer.append(domain_row)

                        if len(buffer) >= BATCH_SIZE:
                            execute_flush(current_high_id)

                except Exception as trans_err:
                    logger.error(f"[{name}] Transformation error at row {current_high_id}: {trans_err}")
                    # Decide here: raise (stop ETL) or continue (skip row)? 
                    # For medical data (OMOP), stopping is usually safer.
                    raise
    

        # Final wrap up for any remaining rows
        if is_batch_mode and raw_buffer:
            batch_results = transform_func(tgt_conn, raw_buffer)
            for transformed, domain_row in batch_results:
                if transformed:
                    buffer.append(transformed)
                if domain_row:
                    domain_buffer.append(domain_row)

        if buffer or domain_buffer:
            execute_flush(current_high_id)

    except Exception as main_err:
        logger.critical(f"[{name}] ETL process halted: {main_err}")
        # Ensure any hanging transactions are cleared
        tgt_conn.rollback()

def run_full_etl():
    src_conn, tgt_conn = get_connections()
    
    try:
        # Define the ETL steps
        steps = [
            
            {
                'name': 'omop.note', # Updated to full schema.table if needed for INSERT
                'query': 'SELECT * '
                    'FROM omop_temp.note '
                    'WHERE note_id > %s '
                    'ORDER BY note_id ASC',
                'transform_func': transform_note,
                'flush_func': generic_flush,
                'is_batch': False # Explicitly mark this as non-batch for clarity
            },
            {
                'name': 'omop.note_nlp',
                'query': 'SELECT note_nlp.*, note.person_id '
                    'FROM omop_temp.note_nlp AS note_nlp '
                    'INNER JOIN omop_temp.note AS note ON note_nlp.note_id = note.note_id '
                    'WHERE note_nlp_id > %s '
                    'ORDER BY note_nlp_id ASC',
                'transform_func': transform_nlp_batch,
                'flush_func': generic_flush,
                'is_batch': True # Mark this step as batch processing
            }
        ]

        for step in steps:
            process_table(src_conn, tgt_conn, step)

    finally:
        src_conn.close()
        tgt_conn.close()

def main():
    run_full_etl()

if __name__ == "__main__":
    main()