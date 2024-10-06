import pandas as pd
import logging
from celery import shared_task
from .models import BookingTransaction, RefundTransaction
from io import BytesIO
import csv

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

BANK_CODE_MAPPING = {
    'hdfc': 101,
    'icici': 102,
    'karur_vysya': 40,
}

BANK_MAPPINGS = {
    'karur_vysya': {
        'booking': {
            'columns': ['TXN DATE', 'IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BOOKING AMOUNT', 'CREDITED ON'],
            'column_mapping': {
                'IRCTCORDERNO': 'irctc_order_no',
                'BANKBOOKINGREFNO': 'bank_booking_ref_no',
                'BOOKINGAMOUNT': 'booking_amount',
                'TXNDATE': 'transaction_date',
                'CREDITEDON': 'credited_date'
            }
        },
        'refund': {
            'columns': ['REFUND DATE', 'IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BANK REFUND REF.NO.', 'REFUND AMOUNT', 'DEBITED ON'],
            'column_mapping': {
                'IRCTCORDERNO': 'irctc_order_no',
                'REFUNDAMOUNT': 'refund_amount',
                'DEBITEDON': 'debited_date',
                'REFUNDDATE': 'refund_date',
                'BANKBOOKINGREFNO': 'bank_booking_ref_no',
                'BANKREFUNDREFNO': 'bank_refund_ref_no'
            }
        }
    }
}

def clean_column_name(column_name):
    parts = column_name.split()
    cleaned_name = ''.join(part for part in parts if part)
    cleaned_name = ''.join(char for char in cleaned_name if char not in ['.', '_']).strip()
    logger.debug(f"Cleaned column name: '{column_name}' to '{cleaned_name}'")
    return cleaned_name

def convert_to_int(val):
    try:
        if pd.isna(val) or val == '':
            return None
        return int(float(val))
    except (ValueError, OverflowError) as e:
        logger.error(f"Error converting value to int: {val}. Error: {e}")
        return None
    
@shared_task
def process_uploaded_files(file_content, file_name, bank_name, transaction_type, file_format):
    try:
        logger.info(f"Received task to process file: {file_name} of type {transaction_type}")

        # Reading the file content
        if file_format == 'excel':
            logger.debug("Reading file as excel.")
            df = pd.read_excel(BytesIO(file_content), dtype=str, engine='openpyxl')
        elif file_format == 'csv':
            logger.debug("Reading file as CSV.")
            df = pd.read_csv(BytesIO(file_content), dtype=str, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        else:
            logger.error(f"Unsupported file format: {file_format}")
            raise ValueError("Unsupported file format")

        logger.info(f"Initial columns found in file {file_name}: {', '.join(df.columns)}")

        df.columns = [clean_column_name(col) for col in df.columns]
        logger.info(f"Cleaned DataFrame columns: {list(df.columns)}")

        expected_columns = [clean_column_name(col) for col in BANK_MAPPINGS[bank_name][transaction_type]['columns']]
        logger.debug(f"Expected columns after cleaning: {expected_columns}")

        missing_columns = set(expected_columns) - set(df.columns)
        if missing_columns:
            logger.warning(f"Missing columns in file {file_name}: {', '.join(missing_columns)}")
            raise ValueError(f"Missing columns after cleaning: {', '.join(missing_columns)}")

        column_mapping = {clean_column_name(k): v for k, v in BANK_MAPPINGS[bank_name][transaction_type]['column_mapping'].items()}
        df = df.rename(columns=column_mapping)
        logger.info(f"Renamed DataFrame columns: {list(df.columns)}")

        bank_code = BANK_CODE_MAPPING.get(bank_name)
        if bank_code is None:
            logger.error(f"Bank code for {bank_name} not found")
            return

        bulk_data_bookings = []
        bulk_data_refunds = []
        
        for index, row in df.iterrows():
            transaction_data = {'bank_code': bank_code}
            try:
                if transaction_type == 'booking':
                    # Add booking-specific information
                    transaction_data.update({
                        'transaction_date': pd.to_datetime(row.get('transaction_date'), errors='coerce'),
                        'credited_date': pd.to_datetime(row.get('credited_date'), errors='coerce'),
                        'booking_amount': float(row.get('booking_amount')) if pd.notnull(row.get('booking_amount')) else 0.0,
                        'irctc_order_no': convert_to_int(row.get('irctc_order_no')),
                        'bank_booking_ref_no': convert_to_int(row.get('bank_booking_ref_no')),
                    })
                    logger.debug(f"Row {index}: Transaction data for booking: {transaction_data}")

                    # Ensure transaction data is valid before adding to bulk
                    if transaction_data['irctc_order_no'] and transaction_data['bank_booking_ref_no'] is not None:
                        bulk_data_bookings.append(transaction_data)  # Append the dict, not an instance
                        logger.info(f"Adding booking transaction: {transaction_data}")

                elif transaction_type == 'refund':
                    # Add refund-specific information
                    transaction_data.update({
                        'refund_date': pd.to_datetime(row.get('refund_date'), errors='coerce'),
                        'bank_booking_ref_no': convert_to_int(row.get('bank_booking_ref_no')),
                        'bank_refund_ref_no': convert_to_int(row.get('bank_refund_ref_no')),
                        'refund_amount': float(row.get('refund_amount')) if pd.notnull(row.get('refund_amount')) else 0.0,
                        'debited_date': pd.to_datetime(row.get('debited_date'), errors='coerce'),
                        'irctc_order_no': convert_to_int(row.get('irctc_order_no')),
                    })
                    logger.debug(f"Row {index}: Transaction data for refund: {transaction_data}")

                    # Ensure transaction data is valid before adding to bulk
                    if transaction_data['irctc_order_no'] and transaction_data['bank_refund_ref_no'] is not None:
                        bulk_data_refunds.append(transaction_data)  # Append the dict, not an instance
                        logger.info(f"Adding refund transaction: {transaction_data}")

            except KeyError as key_err:
                logger.error(f"Missing key in row {index} with data {row.to_dict()}: {str(key_err)}")
            except Exception as ex:
                logger.error(f"Unexpected error processing row {index} with data {row.to_dict()}: {str(ex)}")

        logger.info(f"Prepared {len(bulk_data_bookings)} booking records for database insertion.")
        logger.info(f"Prepared {len(bulk_data_refunds)} refund records for database insertion.")

        if bulk_data_bookings:
            logger.debug("Bulk inserting booking transactions.")
            BookingTransaction.bulk_create_booking_transactions(bulk_data_bookings)  # Now passing a list of dicts
            logger.info(f"Processed and stored {len(bulk_data_bookings)} booking records from file: {file_name}")

        if bulk_data_refunds:
            logger.debug("Bulk inserting refund transactions.")
            RefundTransaction.bulk_create_refund_transactions(bulk_data_refunds)  # Now passing a list of dicts
            logger.info(f"Processed and stored {len(bulk_data_refunds)} refund records from file: {file_name}")

        logger.info("Finished processing the uploaded file.")
    except Exception as e:
        logger.error(f"Error processing file {file_name}: {str(e)}")
        logger.debug(f"Full traceback for error: {e}", exc_info=True)





# import pandas as pd
# import logging
# from celery import shared_task
# from django.db import transaction
# from .models import TransactionData
# from io import BytesIO
# import sys
# import csv

# logging.basicConfig(
#     level=logging.DEBUG,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.StreamHandler(sys.stdout)  # Output to console
#     ]
# )

# logger = logging.getLogger(__name__)

# BANK_CODE_MAPPING = {
#     'hdfc': 101,
#     'icici': 102,
#     'karur_vysya': 40,
# }

# BANK_MAPPINGS = {
#     'karur_vysya': {
#         'booking': {
#             'columns': ['TXN DATE', 'IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BOOKING AMOUNT', 'CREDITED ON'],
#             'column_mapping': {
#                 'IRCTCORDERNO': 'irctc_order_no',
#                 'BANKBOOKINGREFNO': 'bank_booking_ref_no',
#                 'BOOKINGAMOUNT': 'booking_amount',
#                 'TXNDATE': 'transaction_date',
#                 'CREDITEDON': 'credited_date'
#             }
#         },
#         'refund': {
#             'columns': ['REFUND DATE', 'IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BANK REFUND REF.NO.', 'REFUND AMOUNT', 'DEBITED ON'],
#             'column_mapping': {
#                 'IRCTCORDERNO': 'irctc_order_no',
#                 'REFUNDAMOUNT': 'refund_amount',
#                 'DEBITEDON': 'debited_date',
#                 'REFUNDDATE': 'refund_date',
#                 'BANKBOOKINGREFNO': 'bank_booking_ref_no',
#                 'BANKREFUNDREFNO': 'bank_refund_ref_no'
#             }
#         }
#     }
# }

# def clean_column_name(column_name):
#     parts = column_name.split()  # Split by spaces
#     cleaned_name = ''.join(part for part in parts if part)  # Join non-empty parts
#     cleaned_name = ''.join(char for char in cleaned_name if char not in ['.', '_']).strip()  # Clean up
#     logger.debug(f"Cleaned column name: '{column_name}' to '{cleaned_name}'")
#     return cleaned_name

# @shared_task
# def process_uploaded_files(file_content, file_name, bank_name, transaction_type, file_format):
#     try:
#         logger.info(f"Processing file: {file_name} of type {transaction_type}")

#         if file_format == 'excel':
#             df = pd.read_excel(BytesIO(file_content), dtype=str, engine='openpyxl')
#         elif file_format == 'csv':
#             df = pd.read_csv(BytesIO(file_content), dtype=str, quotechar='"', quoting=csv.QUOTE_MINIMAL)
#         else:
#             logger.error(f"Unsupported file format: {file_format}")
#             raise ValueError("Unsupported file format")

#         logger.info(f"Initial columns found in file {file_name}: {', '.join(df.columns)}")

#         df.columns = [clean_column_name(col) for col in df.columns]
#         logger.info(f"Cleaned DataFrame columns: {list(df.columns)}")

        
#         # Log the data contents for debugging
#         logger.info(f"Data contents: {df.head()}") 

#         expected_columns = [clean_column_name(col) for col in BANK_MAPPINGS[bank_name][transaction_type]['columns']]
#         logger.info(f"Expected columns: {expected_columns}")
        
#         missing_columns = set(expected_columns) - set(df.columns)
#         if missing_columns:
#             logger.warning(f"Missing columns in file {file_name}: {', '.join(missing_columns)}")
#             raise ValueError(f"Missing columns: {', '.join(missing_columns)}")

#         column_mapping = {clean_column_name(k): v for k, v in BANK_MAPPINGS[bank_name][transaction_type]['column_mapping'].items()}
#         logger.info(f"Column mapping: {column_mapping}")

#         df = df.rename(columns=column_mapping)
#         logger.info(f"Renamed DataFrame columns: {list(df.columns)}")

#         bank_code = BANK_CODE_MAPPING.get(bank_name)
#         if bank_code is None:
#             logger.error(f"Bank code for {bank_name} not found")
#             return
#         logger.info(f"Bank code for {bank_name}: {bank_code}")

#         with transaction.atomic():
#             bulk_data = []
#             for _, row in df.iterrows():
#                 try:
#                     # Start with common attribute
#                     transaction_data = {
#                         'bank_code': bank_code,
#                     }

#                     if transaction_type == 'booking':
#                         # Fill booking-specific attributes
#                         transaction_data.update({
#                             'transaction_date': pd.to_datetime(row['transaction_date'], errors='coerce'),
#                             'credited_date': pd.to_datetime(row['credited_date'], errors='coerce'),
#                             'booking_amount': float(row['booking_amount']) if row['booking_amount'] else 0.0,  # Ensure float
#                             'irctc_order_no': int(row['irctc_order_no']) if row['irctc_order_no'].isdigit() else None,  # Convert to int
#                             'bank_booking_ref_no': int(row['bank_booking_ref_no']) if row['bank_booking_ref_no'].isdigit() else None,  # Reference for booking
#                             'refund_date': None,  # Not applicable for booking
#                             'bank_refund_ref_no': None,  # Not applicable for booking
#                             'refund_amount': None,  # Not applicable for booking
#                             'debited_date': None,  # Not applicable for booking
#                         })

#                     elif transaction_type == 'refund':
#                         # Fill refund-specific attributes
#                         transaction_data.update({
#                             'debited_date': pd.to_datetime(row['debited_date'], errors='coerce'),  # Parse date
#                             'refund_date': pd.to_datetime(row['refund_date'], errors='coerce'),  # Parse date
#                             'bank_booking_ref_no': int(row['bank_booking_ref_no']) if row['bank_booking_ref_no'].isdigit() else None,  # Reference for booking
#                             'bank_refund_ref_no': int(row['bank_refund_ref_no']) if row['bank_refund_ref_no'].isdigit() else None,  # Reference for refund
#                             'booking_amount': None,  # Not applicable for refunds
#                             'irctc_order_no': int(row['irctc_order_no']) if row['irctc_order_no'].isdigit() else None,  # Keep for refund context
#                             'refund_amount': float(row['refund_amount']) if row['refund_amount'] else 0.0,  # Ensure float
#                         })

#                     # Check for duplicates before adding to bulk_data
#                     if not TransactionData.objects.filter(
#                         irctc_order_no=transaction_data['irctc_order_no'],  # Unique field for booking and refund
#                         bank_booking_ref_no=transaction_data.get('bank_booking_ref_no')  # Unique ref for booking
#                     ).exists():
#                         bulk_data.append(TransactionData(**transaction_data))  # Store the transaction data instance
#                     else:
#                         logger.warning(f"Duplicate entry found for IRCTC Order No: {transaction_data['irctc_order_no']} and Booking Ref No: {transaction_data.get('bank_booking_ref_no')}. It will be skipped.")

#                 except KeyError as key_error:
#                     logger.error(f"Missing key in row {row}: {str(key_error)}")  # Log missing keys
#                 except Exception as row_error:
#                     logger.error(f"Error processing row {row}: {str(row_error)}")  # Log errors with processing rows

#             logger.info(f"Prepared {len(bulk_data)} records for database insertion.")
#             TransactionData.objects.bulk_create(bulk_data)  # Efficient bulk insert into the database
#             logger.info(f"Processed and stored {len(bulk_data)} records from file: {file_name}")

#     except Exception as e:
#         logger.error(f"Error processing file: {file_name}, Error: {str(e)}")  # Log errors during processing
