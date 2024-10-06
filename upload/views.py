
# Create your views here.
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.core.paginator import Paginator
from .forms import UploadFileForm
from .tasks import process_uploaded_files
from .models import BookingTransaction, RefundTransaction
from django.db.models import Sum


def upload_files(request):
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            bank_name = form.cleaned_data['bank_name']
            transaction_type = form.cleaned_data['transaction_type']
            files = request.FILES.getlist('file')
            
            for file in files:
                file_content = file.read()
                print(file_content)
                file_name = file.name
                print(file_name)
                file_format = 'excel' if file.name.endswith('.xlsx') else 'csv'
                process_uploaded_files.delay(file_content, file_name, bank_name, transaction_type, file_format)
            
            return JsonResponse({'message': 'Files uploaded successfully. Processing started.'}, status=202)
    else:
        form = UploadFileForm()
    
    return render(request, 'upload.html', {'form': form})


# def transaction_list(request):
#     transactions = TransactionData.objects.all().order_by('-date')
#     paginator = Paginator(transactions, 20)  # Show 20 transactions per page
#     page_number = request.GET.get('page')
#     page_obj = paginator.get_page(page_number)
    
#     total_amount = TransactionData.objects.aggregate(Sum('amount'))['amount__sum']
    
#     context = {
#         'page_obj': page_obj,
#         'total_amount': total_amount,
#     }
#     return render(request, 'transaction_list.html', context)








# import oracledb

# # Establish a connection to the production Oracle database
# with oracledb.connect(user='PGACT7', password='Oct2024', dsn='10.78.14.42:1725/rptdb_srv.cris.org.in') as connection:
#     with connection.cursor() as cursor:
#         # Use FETCH FIRST to limit the rows (Oracle SQL syntax)
#         sql = """SELECT "PAYMENT_DATE", "ENTITY_ID", "AMOUNT", "BANK_ID" 
#                  FROM TRANSACTION_DB.ET_PAYMENT_CASH 
#                  WHERE "BANK_ID" = 40 AND ROWNUM <= 2"""

#         # Execute the query
#         cursor.execute(sql)
#         # Fetch and print the results
#         for row in cursor:
#             print(row)