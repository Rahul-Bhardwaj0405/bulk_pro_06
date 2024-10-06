from django.db import models, transaction
from django.core.exceptions import ValidationError
import logging

logger = logging.getLogger(__name__)

class BookingTransaction(models.Model):
    bank_code = models.IntegerField()
    transaction_date = models.DateField(blank=True, null=True)
    credited_date = models.DateField(blank=True, null=True)
    irctc_order_no = models.BigIntegerField(blank=True, null=True, unique=True)
    bank_booking_ref_no = models.BigIntegerField(blank=True, null=True, unique=True)
    booking_amount = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)

    class Meta:
        indexes = [
            models.Index(fields=['bank_code']),
            models.Index(fields=['irctc_order_no', 'bank_booking_ref_no']),
        ]

    def __str__(self):
        return (
            f"IRCTC Order No: {self.irctc_order_no}, "
            f"Bank Booking Ref No: {self.bank_booking_ref_no}, "
            f"Bank Code: {self.bank_code}, "
            f"Booking Amount: {self.booking_amount}"
        )

    def clean(self):
        # Model-level validation
        if self.booking_amount is not None and self.booking_amount < 0:
            raise ValidationError('Booking amount cannot be negative.')
        if not self.transaction_date or not self.credited_date:
            raise ValidationError('Both transaction date and credited date must be provided.')

    @classmethod
    def bulk_create_booking_transactions(cls, transactions):
        existing_orders = set(cls.objects.filter(
            irctc_order_no__in={tx['irctc_order_no'] for tx in transactions if 'irctc_order_no' in tx}
        ).values_list('irctc_order_no', flat=True))

        new_transactions = [tx for tx in transactions if tx.get('irctc_order_no') not in existing_orders]

        if new_transactions:
            batch_size = 500
            for i in range(0, len(new_transactions), batch_size):
                try:
                    with transaction.atomic():
                        cls.objects.bulk_create(
                            [cls(**transaction) for transaction in new_transactions[i:i + batch_size]],
                            batch_size=batch_size,
                            ignore_conflicts=True
                        )
                except Exception as ex:
                    # Log the exception and raise or handle it according to your needs
                    logger.error(f"Error during bulk create: {str(ex)}")
                    raise  # Optionally re-raise the exception if needed
            
            return len(new_transactions)
        return 0


class RefundTransaction(models.Model):
    bank_code = models.IntegerField()
    refund_date = models.DateField(blank=True, null=True)
    bank_booking_ref_no = models.BigIntegerField(blank=True, null=True, unique=True)
    bank_refund_ref_no = models.BigIntegerField(blank=True, null=True, unique=True)
    refund_amount = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    debited_date = models.DateField(blank=True, null=True)
    irctc_order_no = models.BigIntegerField(blank=True, null=True)

    class Meta:
        indexes = [
            models.Index(fields=['bank_code']),
            models.Index(fields=['irctc_order_no', 'bank_booking_ref_no', 'bank_refund_ref_no']),
        ]

    def __str__(self):
        return (
            f"IRCTC Order No: {self.irctc_order_no}, "
            f"Bank Refund Ref No: {self.bank_refund_ref_no}, "
            f"Bank Booking Ref No: {self.bank_booking_ref_no}, "
            f"Refund Amount: {self.refund_amount}"
        )

    def clean(self):
        # Model-level validation
        if self.refund_amount is not None and self.refund_amount < 0:
            raise ValidationError('Refund amount cannot be negative.')
        if not self.refund_date or not self.debited_date:
            raise ValidationError('Both refund date and debited date must be provided.')

    @classmethod
    def bulk_create_refund_transactions(cls, transactions):
        existing_orders = set(cls.objects.filter(
            irctc_order_no__in={tx['irctc_order_no'] for tx in transactions if 'irctc_order_no' in tx}
        ).values_list('irctc_order_no', flat=True))

        new_transactions = [tx for tx in transactions if tx.get('irctc_order_no') not in existing_orders]

        if new_transactions:
            batch_size = 500
            for i in range(0, len(new_transactions), batch_size):
                try:
                    with transaction.atomic():
                        cls.objects.bulk_create(
                            [cls(**transaction) for transaction in new_transactions[i:i + batch_size]],
                            batch_size=batch_size,
                            ignore_conflicts=True
                        )
                except Exception as ex:
                    # Log the exception and raise or handle it according to your needs
                    logger.error(f"Error during bulk create: {str(ex)}")
                    raise  # Optionally re-raise the exception if needed
            
            return len(new_transactions)
        return 0
